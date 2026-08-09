# -*- coding: utf-8 -*-

"""The JobServer for the SEAMM environment."""

import collections.abc
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path, PurePosixPath
import psutil
import shlex
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import traceback

import seamm_jobserver
import seamm_util
from seamm_slurm.config import load_slurm_config
from seamm_slurm.script import build_script
from seamm_slurm.stage import StageError

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def run():
    """Helper routine to run the JobServer from the command-line"""
    jobserver = seamm_jobserver.JobServer()
    jobserver.start()


def humanize(memory, suffix="B", kilo=1024):
    """
    Scale memory to its proper format e.g:

        1253656 => '1.20 MiB'
        1253656678 => '1.17 GiB'
    """
    if kilo == 1000:
        units = ["", "k", "M", "G", "T", "P"]
    elif kilo == 1024:
        units = ["", "Ki", "Mi", "Gi", "Ti", "Pi"]
    else:
        raise ValueError("kilo must be 1000 or 1024!")

    for unit in units:
        if memory < kilo:
            return f"{memory:.2f} {unit}{suffix}"
        memory /= kilo


class TkTextHandler(logging.StreamHandler):
    def __init__(self, widget):
        super().__init__()
        self.text = widget

    def emit(self, record):
        msg = self.format(record)
        self.text.insert("end", msg)
        self.text.insert("end", "\n")


class JobServer(collections.abc.MutableMapping):
    def __init__(self, logger=logger):
        """Initialize the instance

        Parameters
        ----------
        check_interval : integer
            Number of seconds between checks for new jobs in the database
        """
        super().__init__()

        self.check_interval = 1
        self.status_interval = 5
        self.logger = logger
        self.options = None
        self.seamm_options = None
        self.stop = False
        self.total_jobs = 0
        self.previous_jobs = 0
        self.successful_jobs = 0
        self.ended_jobs = 0
        self.failed_jobs = 0
        self.killed_jobs = 0

        self._db = None
        self._db_path = None
        self._tasks = set()
        self._jobs = {}
        self._slurm = None
        self._slurm_backend = None
        self._stager = None
        self._tk_root = None
        self._after_id = None
        self._status_id = None
        self._widget = {}
        self._times = {"JobServer": {}}

    # Provide dict like access to the widgets to make the code cleaner
    def __getitem__(self, key):
        """Allow [] access to the widgets."""
        return self._widget[key]

    def __setitem__(self, key, value):
        """Allow [key] access to set a widget."""
        self._widget[key] = value

    def __delitem__(self, key):
        """Allow deletion of widgets."""
        if key in self._widget:
            self._widget[key].destroy()
        del self._widget[key]

    def __iter__(self):
        """Allow iteration over the widgets"""
        return iter(self._widget)

    def __len__(self):
        """Provide the nmber of widgets, for e.g. len() command."""
        return len(self._widget)

    @property
    def db_path(self):
        return self._db_path

    @db_path.setter
    def db_path(self, value):
        if value != self._db_path:
            # Close any connection to the database
            if self._db is not None:
                self._db.close()
                self._db = None
            if value is not None:
                self.logger.info(f"Opening the database '{value}'")
                self._db = sqlite3.connect(value)
            self._db_path = value

    @property
    def db(self):
        return self._db

    def check_for_stopped_jobs(self):
        """Look for jobs that should be actively stopped: either their
        datastore row vanished entirely (deleted, e.g. via the dashboard),
        or their status was explicitly set to ``kill`` (stop the run but
        leave the job's files/row alone, unlike delete).

        Before SLURM, a deleted job's files disappeared but nothing told
        the local subprocess to stop -- it just ran on until it crashed on
        the missing files. On a scheduler that is not good enough: an
        orphaned SLURM job can sit running (or queued) for hours. So every
        poll cycle, before ``check_for_finished_jobs`` gets a chance to
        treat a just-killed job as merely lost and try to resubmit it, this
        checks every job actually being tracked and every ``kill``-status
        row, and issues the real ``scancel``/process-kill.
        """
        if self._jobs:
            job_ids = list(self._jobs.keys())
            placeholders = ",".join("?" * len(job_ids))
            cursor = self.db.cursor()
            cursor.execute(f"SELECT id FROM jobs WHERE id IN ({placeholders})", job_ids)
            present = {row[0] for row in cursor.fetchall()}
            for job_id in job_ids:
                if job_id not in present:
                    self.logger.warning(
                        f"Job {job_id}'s datastore row was deleted; stopping it."
                    )
                    self._stop_running_job(job_id)

        cursor = self.db.cursor()
        cursor.execute("SELECT id FROM jobs WHERE status = 'kill'")
        for (job_id,) in cursor.fetchall():
            if job_id in self._jobs:
                self.logger.info(f"Job {job_id} was asked to stop; stopping it.")
                self._stop_running_job(job_id)
            self._finalize_job_status(job_id, "killed", expected_current="kill")

    def _stop_running_job(self, job_id):
        """Actively terminate a tracked job's local process, or cancel its
        SLURM job, and stop tracking it here. Does not touch the datastore
        row -- callers decide what, if anything, to write there."""
        data = self._jobs.pop(job_id, None)
        self._times.pop(job_id, None)
        if data is None:
            return

        self.killed_jobs += 1
        if data["mode"] == "local":
            process = data["process"]
            try:
                process.terminate()
                process.wait(timeout=5)
            except psutil.TimeoutExpired:
                process.kill()
            except Exception as e:
                self.logger.warning(f"Error stopping local job {job_id}: {e}")
        else:
            slurm_id = data["slurm_job_id"]
            try:
                self._slurm_backend.cancel(slurm_id)
            except Exception as e:
                self.logger.warning(
                    f"Error cancelling SLURM job {job_id} ({slurm_id}): {e}"
                )

    def check_for_finished_jobs(self):
        """Check whether jobs have finished."""
        if self._slurm is not None:
            self._check_for_finished_jobs_slurm()
        else:
            self._check_for_finished_jobs_local()

    def _check_for_finished_jobs_local(self):
        """Check whether locally-run (non-SLURM) jobs have finished, and
        finalize their datastore status here -- the job's own process no
        longer writes it directly (see Phase 8 in the campaign doc: the
        JobServer, not the job, now owns every terminal status write, the
        same as SLURM mode already required)."""
        finished = []
        for job_id, data in self._jobs.items():
            pid = data["pid"]
            process = data["process"]
            try:
                is_running = process.is_running()
                if process.status() == psutil.STATUS_ZOMBIE:
                    is_running = False
            except Exception:
                is_running = False
            if is_running:
                self.logger.debug(f"Job {job_id} is running as process {pid}")
                continue

            finished.append(job_id)

            # Trust job_data.json -- the flowchart's own conclusion --
            # over the exit code when both are available; the exit code is
            # only a fallback for a hard crash that never got to write it.
            state = self._read_job_data_state(data["wdir"])
            bucket = None
            if state is not None:
                self.logger.info(f"Job {job_id} finished ({pid=}) -> {state!r}.")
                bucket = "successful" if state == "finished" else "failed"
            else:
                try:
                    returncode = process.returncode
                except Exception:
                    returncode = "unknown"
                self.logger.debug(f"Job {job_id}: no job_data.json, code={returncode}.")
                if returncode is None or returncode == 0:
                    state = "finished"
                    bucket = "successful"
                    self.logger.info(f"Job {job_id} finished successfully ({pid=}).")
                elif returncode == "unknown":
                    # Can't confirm success -- write "error" so the row
                    # doesn't stay stuck at "running" forever, but keep it
                    # in its own counter bucket since it's not a confirmed
                    # failure either.
                    state = "error"
                    bucket = "ended"
                    self.logger.info(
                        f"Job {job_id} finished with unknown status ({pid=})."
                    )
                else:
                    state = "error"
                    bucket = "failed"
                    self.logger.info(f"Job {job_id} failed ({pid=} {returncode=}).")

            self._finalize_job_status(job_id, state)
            if bucket == "successful":
                self.successful_jobs += 1
            elif bucket == "ended":
                self.ended_jobs += 1
            else:
                self.failed_jobs += 1
        for job_id in finished:
            del self._jobs[job_id]
            del self._times[job_id]

    def _check_for_finished_jobs_slurm(self):
        """Check whether SLURM-submitted jobs have finished.

        SLURM is the source of truth for *whether* a job has finished; the
        JobServer is the sole writer of the datastore's terminal status for
        every job (see Phase 8 in the campaign doc -- the job's own process
        used to write this directly, which both duplicated this logic and
        cannot work at all once a job may run on a host with no access to
        the datastore's sqlite file). ``_finalize_or_resubmit_slurm_job``
        does the actual finalize-or-resubmit work, trusting
        ``job_data.json`` over SLURM's own completed/failed category when
        both are available -- SLURM reporting a job "completed" only means
        the process exited 0, not that the flowchart itself considered the
        run successful.
        """
        if not self._jobs:
            return

        id_map = {data["slurm_job_id"]: job_id for job_id, data in self._jobs.items()}
        statuses = self._slurm_backend.poll_many(list(id_map.keys()))

        # Snapshot: _finalize_or_resubmit_slurm_job may resubmit, which
        # mutates self._jobs/_times for job_id in place (new slurm_job_id)
        # -- don't iterate the live dict while that happens.
        finished = []
        for job_id, data in list(self._jobs.items()):
            slurm_id = data["slurm_job_id"]
            status = statuses.get(slurm_id)

            if status is not None and not status.is_terminal:
                self.logger.debug(
                    f"Job {job_id} (slurm_job_id={slurm_id}) is {status.category}."
                )
                continue

            if status is None:
                self.logger.warning(
                    f"Job {job_id} (slurm_job_id={slurm_id}) is no longer "
                    "known to SLURM."
                )
            else:
                self.logger.info(
                    f"Job {job_id} (slurm_job_id={slurm_id}) reached SLURM "
                    f"state {status.state} ({status.category})."
                )

            if self._get_job_status(job_id) == "running":
                resubmitted = self._finalize_or_resubmit_slurm_job(
                    job_id, data, data.get("resubmit_count", 0)
                )
                if resubmitted:
                    # _start_job_slurm already replaced self._jobs[job_id]/
                    # self._times[job_id] with fresh tracking data -- keep it.
                    continue

            finished.append(job_id)

        for job_id in finished:
            del self._jobs[job_id]
            del self._times[job_id]

    def _get_job_status(self, job_id):
        """The current ``jobs.status`` value for a job, from the datastore."""
        cursor = self.db.cursor()
        cursor.execute("SELECT status FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    @staticmethod
    def _read_job_data_state(wdir):
        """Read the ``state`` field from ``<wdir>/job_data.json``, if any.

        ``run()``'s ``finally`` block writes this file unconditionally,
        before it attempts its own direct sqlite update -- so it is often
        available even when that later, racier write didn't happen.
        """
        path = Path(wdir) / "job_data.json"
        if not path.exists():
            return None
        try:
            text = path.read_text()
            # Strip the "!MolSSI job_data 1.0" header. Normally on its own
            # line, but some writers (seamm_exec's own exception handler,
            # historically) have omitted the newline after it -- so look for
            # the JSON object's opening brace rather than assuming a full
            # first line, to tolerate both forms.
            text = text[text.index("{") :]
            data = json.loads(text)
            return data.get("state")
        except (OSError, ValueError, KeyError):
            return None

    def _finalize_job_status(self, job_id, status, expected_current="running"):
        """Force a job's datastore status, but only if it still has the
        expected current status -- avoids clobbering a status something
        else (the job's own process, another poll cycle) already wrote."""
        current_time = datetime.now(timezone.utc)
        cursor = self.db.cursor()
        cursor.execute(
            "UPDATE jobs SET status = ?, finished = ?" " WHERE id = ? AND status = ?",
            (status, current_time, job_id, expected_current),
        )
        self.db.commit()

    def _finalize_or_resubmit_slurm_job(self, job_id, data, resubmit_count):
        """Finalize a SLURM job's datastore status now that SLURM reports it
        terminal (or has no record of it at all), or resubmit it if there's
        nothing yet to finalize from.

        For a ``transport = ssh`` job (``data["remote_wdir"]`` set), the
        remote working directory is pulled back to ``wdir`` first (via
        ``self._stager``), so ``job_data.json`` and the flowchart's own
        results land where the JobServer -- and the dashboard -- can see
        them. A stage-out failure is a transient transfer problem, not a
        job failure: it's worth a retry next poll cycle, not conflated
        with the resubmit-count logic below (which is about SLURM losing
        track of the job entirely, a different failure mode).

        First choice: trust ``job_data.json`` if the flowchart's own process
        managed to write one -- this is the common case (a normal finish),
        not just a stalled-job fallback, since the JobServer is now the
        only thing that writes the datastore's terminal status. Falls back
        to resubmitting, up to ``max_resubmits``, since flowcharts
        checkpoint completed steps and safely resume (see the design plan);
        beyond the cap, gives up and marks the job ``error``.

        Returns
        -------
        bool
            True if the job should stay tracked -- either resubmitted
            (``self._jobs[job_id]``/``self._times[job_id]`` were just
            replaced with fresh tracking data by ``_start_job_slurm``) or a
            stage-out failure worth retrying next cycle (tracking data left
            untouched). False if the job was finalized (finished/error) and
            is no longer tracked here.
        """
        wdir = data["wdir"]
        remote_wdir = data.get("remote_wdir")

        if remote_wdir is not None:
            try:
                self._stager.stage_out(remote_wdir, wdir)
            except StageError as e:
                self.logger.warning(
                    f"Job {job_id}: stage_out from {remote_wdir} failed, "
                    f"will retry next poll cycle: {e}"
                )
                return True

        state = self._read_job_data_state(wdir)
        if state is not None:
            self.logger.info(f"Job {job_id}: finalizing as {state!r} (job_data.json).")
            self._finalize_job_status(job_id, state)
            if state == "finished":
                self.successful_jobs += 1
            else:
                self.failed_jobs += 1
            return False

        max_resubmits = self._slurm.max_resubmits
        if resubmit_count >= max_resubmits:
            self.logger.warning(
                f"Job {job_id}: giving up after {resubmit_count} resubmit(s) "
                "(SLURM lost track of it and no job_data.json was written)."
            )
            self._finalize_job_status(job_id, "error")
            self.failed_jobs += 1
            return False

        self.logger.warning(
            f"Job {job_id}: resubmitting (attempt {resubmit_count + 1}/"
            f"{max_resubmits})."
        )
        self._start_job_slurm(
            job_id,
            wdir,
            data["cmdline"],
            resubmit_count=resubmit_count + 1,
            slurm_overrides=data.get("slurm_overrides"),
        )
        return True

    def check_for_new_jobs(self):
        """Check the database for new jobs that are runnable."""
        query = (
            "SELECT id, path, json_extract(parameters, '$.cmdline'),"
            "       json_extract(parameters, '$.slurm')"
            "  FROM jobs"
            " WHERE status = 'submitted'"
        )
        params = ()

        if self._slurm is not None:
            available = self._slurm.max_concurrent_jobs - len(self._jobs)
            if available <= 0:
                self.logger.debug(
                    "At max_concurrent_jobs "
                    f"({self._slurm.max_concurrent_jobs}); not starting new "
                    "jobs this cycle."
                )
                return
            query += " LIMIT ?"
            params = (available,)

        cursor = self.db.cursor()

        self.logger.debug("Checking jobs in datastore")
        cursor.execute(query, params)
        while True:
            result = cursor.fetchone()
            if result is None:
                break
            job_id, path, cmdline, slurm_overrides = result
            cmdline = json.loads(cmdline)
            slurm_overrides = json.loads(slurm_overrides) if slurm_overrides else {}

            try:
                pid = self.start_job(
                    job_id, path, cmdline, slurm_overrides=slurm_overrides
                )
            except Exception as e:
                self.logger.warning(f"An error occurred starting job {job_id}:\n\t{e}")
                status = "startup error"
                current_time = datetime.now(timezone.utc)
                cursor = self.db.cursor()
                cursor.execute(
                    "UPDATE jobs"
                    "   SET status=?, started = ?, finished = ?"
                    " WHERE id = ?",
                    (status, current_time, current_time, job_id),
                )
                self.db.commit()
            else:
                if self._slurm is None:
                    status = "running"
                    current_time = datetime.now(timezone.utc)
                    cursor = self.db.cursor()
                    cursor.execute(
                        "UPDATE jobs"
                        "   SET status=?, started = ?,"
                        "       parameters=json_set(jobs.parameters, '$.pid', ?)"
                        " WHERE id = ?",
                        (status, current_time, pid, job_id),
                    )
                    self.db.commit()

                    self.logger.info(
                        f"Started job {job_id} with pid={pid}, path={path}"
                    )
                else:
                    # _start_job_slurm already updated the datastore itself.
                    self.logger.info(f"Started job {job_id} via SLURM, path={path}")

    def gui_create(self):
        """Create the tkinter GUI."""
        import tkinter as tk
        import tkinter.ttk as ttk
        from tkinter.scrolledtext import ScrolledText

        # Initialize Tk
        self._tk_root = tk.Tk()
        self._tk_root.protocol("WM_DELETE_WINDOW", self.gui_on_closing)

        app_name = f"JobServer {self.seamm_options['root']}"
        self._tk_root.title(app_name)

        # The menus
        menu = tk.Menu(self._tk_root)

        # Set the about and preferences menu items on Mac
        if sys.platform.startswith("darwin"):
            app_menu = tk.Menu(menu, name="apple")
            menu.add_cascade(menu=app_menu)

            app_menu.add_command(label="About " + app_name, command=self.gui_about)
            app_menu.add_separator()
            self._tk_root.createcommand(
                "tk::mac::ShowPreferences", self.gui_preferences
            )
            # self._tk_root.createcommand(
            #     "tk::mac::OpenDocument", tk_flowchart.open_file
            # )
            self.CmdKey = "Command-"
        else:
            self.CmdKey = "Control-"

        notebook = ttk.Notebook(self._tk_root)
        notebook.grid(row=0, column=0, columnspan=2, sticky=tk.NSEW)
        self["notebook"] = notebook
        self._tk_root.rowconfigure(0, weight=1)
        self._tk_root.columnconfigure(0, weight=1)

        # Button for exiting the JobServer
        w = ttk.Button(self._tk_root, text="Exit", command=self.gui_on_closing)
        w.grid(row=1, column=1, sticky=tk.E)

        # Tab for the log
        self["log frame"] = frame = ttk.Frame(notebook)
        notebook.add(frame, text="Log", sticky=tk.NSEW)

        # Add a scrolled text area for logging
        self["log"] = log = ScrolledText(frame, wrap=tk.WORD, font=("TkFixedFont",))
        log.grid(row=0, column=0, columnspan=2, sticky=tk.NSEW)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        w = ttk.Button(frame, text="Clear", command=lambda: log.delete("1.0", "end"))
        w.grid(row=1, column=0, sticky=tk.W)

        # Insert the initial log info into the widget
        logfile = self.options["log_file"]
        self["log"].insert("end", f"The JobServer is starting in {Path.cwd()}\n")
        self["log"].insert(
            "end", f"           version = {seamm_jobserver.__version__}\n"
        )
        self["log"].insert("end", f"         datastore = {self.db_path}\n")
        self["log"].insert("end", f"    check interval = {self.check_interval}\n")
        self["log"].insert("end", f"          log file = {logfile}\n")

        if not self.options["no_windows"]:
            self["log"].insert("end", "Using the GUI.\n")

        if len(self._ini_files) == 0:
            self["log"].insert("end", "No .ini files were used\n")
        else:
            self["log"].insert("end", "The following .ini files were used:\n")
            for filename in self._ini_files:
                self["log"].insert("end", f"    {filename}\n")
        self["log"].insert("end", "\n")

        # And set up logging to echo to the log widget
        th = TkTextHandler(self["log"])
        formatter = logging.Formatter("%(message)s")
        th.setFormatter(formatter)
        th.setLevel(logging.INFO)
        self.logger.addHandler(th)

        # Tab for the status
        self["status frame"] = frame = ttk.Frame(notebook)
        notebook.add(frame, text="Status", sticky=tk.NSEW)

        # Add a scrolled text area for logging
        self["status"] = w = ScrolledText(frame, wrap=tk.WORD, font=("TkFixedFont",))
        w.grid(row=0, column=0, columnspan=2, sticky=tk.NSEW)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        # Fill the screen
        sw = self._tk_root.winfo_screenwidth()
        sh = self._tk_root.winfo_screenheight()
        width = int(0.9 * sw) - 50
        height = int(0.9 * sh) - 50
        x = int(0.1 * sw / 2)
        y = int(0.1 * sh / 2)
        self._tk_root.geometry(f"{width}x{height}+{x}+{y}")

    def gui_about(self):
        """Provide information about the JobServer."""
        from tkinter import messagebox

        messagebox.showinfo(
            "About SEAMM JobServer", f"SEAMM JobServer v{seamm_jobserver.__version__}"
        )

    def gui_event_loop(self):
        """The callback for the main loop when using Tk"""
        if self.stop:
            self._tk_root.quit()
        try:
            self.check_for_stopped_jobs()
            self.check_for_finished_jobs()
            self.check_for_new_jobs()
        except Exception as e:
            self.logger.error(f"Error: {e}\n\n{traceback.format_exc()}")
        self._after_id = self._tk_root.after(
            int(self.check_interval * 1000), self.gui_event_loop
        )

    def gui_on_closing(self):
        """Check that the user wants to stop the JobServer, and do so"""
        from tkinter import messagebox

        if messagebox.askyesno("Exit", "Do you want to exit the JobServer?"):
            self.stop = True

    def gui_preferences(self):
        """Provide access to the preferences for the JobServer."""
        from tkinter import messagebox

        messagebox.showinfo(
            "SEAMM JobServer Preferences", "Currently there are no preferences"
        )

    def gui_status(self, status):
        """Display the current load and jobs."""
        text = self["status"]
        text.delete("1.0", "end")

        text.insert("end", f"Status at {status['time']}\n\n")
        text.insert("end", f"Jobs previously running: {status['previous jobs']:4d}\n")
        text.insert("end", f"                started: {status['total jobs']:4d}\n")
        text.insert("end", f" completed successfully: {status['successful jobs']:4d}\n")
        text.insert("end", f"                 failed: {status['failed jobs']:4d}\n")
        text.insert("end", f"                 killed: {status['killed jobs']:4d}\n")
        text.insert("end", f"         unknown status: {status['ended jobs']:4d}\n\n")

        # Cpu usage on machine
        text.insert("end", f"  User time: {status['machine user time']:10.1f}\n")
        text.insert("end", f"System time: {status['machine system time']:10.1f}\n")
        text.insert("end", f"  Idle time: {status['machine idle time']:10.1f}\n")
        text.insert("end", f"      CPU %: {status['machine % cpu']:10.1f}\n")
        available = status["available memory"]
        pct = status["memory % used"]
        total = status["total memory"]
        text.insert("end", f" Memory available: {available}  {pct:5.1f}%")
        text.insert("end", f"            total: {total}")
        text.insert("end", "\n\n")

        # The jobserver itself
        if "JobServer" in status:
            js = status["JobServer"]
            cpu_percent = js["cpu %"]
            cpu = js["cpu time"]
            rss = js["resident memory"]
            memory_percent = js["memory %"]
            text.insert(
                "end",
                f"JobServer: cpu {cpu_percent:.1f}% {cpu:.1f} "
                f"memory {rss} {memory_percent:.1f}%\n",
            )

        for job_id in sorted(status["Jobs"].keys()):
            js = status["Jobs"][job_id]
            if "slurm_job_id" in js:
                text.insert(
                    "end",
                    f"\n{job_id}: slurm_job_id={js['slurm_job_id']}\n",
                )
                continue
            memory_percent = js["memory %"]
            rss = js["resident memory"]
            cpu_percent = js["cpu %"]
            cpu = js["cpu time"]

            text.insert(
                "end",
                f"\n{job_id}: cpu {cpu_percent:.1f}% {cpu:.1f} "
                f"memory {rss} {memory_percent:.1f}%\n",
            )
            if "sub processes" in js:
                for pid in sorted(js["sub processes"].keys()):
                    sub = js["sub processes"][pid]
                    memory_percent = sub["memory %"]
                    rss = sub["resident memory"]
                    cpu_percent = sub["cpu %"]
                    cpu = sub["cpu time"]
                    name = sub["name"]
                    text.insert(
                        "end",
                        f"    {pid}: {name} cpu {cpu_percent:.1f}% "
                        f"{cpu:.1f} memory {rss} {memory_percent:.1f}%\n",
                    )

    def gui_status_loop(self):
        """The callback for the the status."""
        try:
            status = self.status()

            status_file = self.options["status_file"]
            if status_file != "none":
                status_file = Path(status_file).expanduser()
                with open(status_file, "w") as fd:
                    json.dump(status, fd, indent=4)

            self.gui_status(status)
        except Exception as e:
            self.logger.error(f"Error: {e}\n\n{traceback.format_exc()}")
        self._status_id = self._tk_root.after(
            int(self.status_interval * 1000), self.gui_status_loop
        )

    def initialize(self):
        """Parse the command-line and setup the JobServer"""
        parser = self.setup_parser()
        parser.parse_args()
        self.options = parser.get_options("JobServer")
        self.seamm_options = parser.get_options("SEAMM")

        # Make sure the logs folder exists (avoid FileNotFoundError)
        logfile = Path(self.options["log_file"]).expanduser()

        # Set the logging level for the JobServer itself
        logger.setLevel(self.options["log_level"])

        # create file handler
        fh = logging.FileHandler(logfile)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
        fh.setFormatter(formatter)
        # add the handlers to the logger
        self.logger.addHandler(fh)

        # Where is the datastore?
        datastore = Path(self.seamm_options["datastore"]).expanduser()

        # Get the database file / instance
        db_path = datastore / "seamm.db"

        self.check_interval = self.options["check_interval"]

        # SLURM config, if any -- <root>/<jobserver-name>.ini. Absence means
        # "run jobs as local subprocesses", exactly as before this existed.
        root = Path(self.seamm_options["root"]).expanduser()
        self._slurm = load_slurm_config(root, self.options["name"])
        if self._slurm is not None:
            self._slurm_backend = self._slurm.build_backend()
            self._stager = self._slurm.build_stager()
            logger.info(
                f"Using SLURM (section '{self._slurm.name}', transport="
                f"'{self._slurm.transport}') to run jobs, up to "
                f"{self._slurm.max_concurrent_jobs} at a time."
            )

        # Log how we are starting
        self._ini_files = parser.get_ini_files()
        logger.info(f"The JobServer is starting in {Path.cwd()}")
        logger.info(f"           version = {seamm_jobserver.__version__}")
        logger.info(f"         datastore = {db_path}")
        logger.info(f"    check interval = {self.check_interval}")
        logger.info(f"          log file = {logfile}")

        if not self.options["no_windows"]:
            logger.info("Using the GUI.")

        if len(self._ini_files) == 0:
            logger.info("No .ini files were used")
        else:
            logger.info("The following .ini files were used:")
            for filename in self._ini_files:
                logger.info(f"    {filename}")
        logger.info("")

        # And create the GUI if needed.
        if not self.options["no_windows"]:
            self.gui_create()

        # Open the database
        self.db_path = db_path

    def setup_parser(self):
        """Setup the command-line parser."""
        parser = seamm_util.seamm_parser("JobServer")

        parser.add_parser("JobServer")

        parser.add_argument(
            "SEAMM",
            "--version",
            action="version",
            version=f"JobServer version {seamm_jobserver.__version__}",
        )

        parser.add_argument(
            "JobServer",
            "--log-level",
            default="INFO",
            type=str.upper,
            choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help=(
                "The level of informational output for jobs, defaults to '%(default)s'"
            ),
        )

        parser.add_argument(
            "JobServer",
            "--no-windows",
            "-nw",
            action="store_true",
            help="Don't use a graphical interface.",
        )

        parser.add_argument(
            "JobServer",
            "--check-interval",
            type=float,
            default=1.0,
            action="store",
            help="The interval for checking for new jobs.",
        )

        parser.add_argument(
            "JobServer",
            "--log-file",
            default="${SEAMM:root}/logs/jobserver.log",
            action="store",
            help="Where to save the logs.",
        )

        parser.add_argument(
            "JobServer",
            "--status-file",
            default="${SEAMM:root}/logs/jobserver_status.json",
            action="store",
            help="Where to save the JSON of the status.",
        )

        parser.add_argument(
            "JobServer",
            "--name",
            default=socket.gethostname(),
            action="store",
            help="The name of the JobServer.",
        )

        return parser

    def start(self):
        """Start the main event loop."""
        if self.options is None:
            self.initialize()

        # Find any jobs already running. A 'running'/'kill' row is a local
        # pid or a SLURM submission based on what was actually recorded for
        # *that* job, not on whether this JobServer instance is currently
        # SLURM-configured -- a row can predate this instance's SLURM config
        # (e.g. its first restart after SLURM was just enabled). So always
        # check for local pids first (regardless of mode), then -- only if
        # SLURM is configured, since polling needs a backend -- reconcile
        # whatever's left (real SLURM submissions, or rows with neither a
        # pid nor a slurm_job_id recorded at all). _reattach_local_jobs
        # leaves alone (does not finalize) any row that also has a
        # slurm_job_id, so the two passes never fight over the same row.
        self._reattach_local_jobs()
        if self._slurm is not None:
            self._reattach_slurm_jobs()

        if self._tk_root is not None:
            self._after_id = self._tk_root.after(10, self.gui_event_loop)
            self._status_id = self._tk_root.after(int(1000), self.gui_status_loop)
            self._tk_root.mainloop()
        else:
            while not self.stop:
                # If nothing to do sleep and then check for new jobs
                if len(self._tasks) == 0:
                    time.sleep(self.check_interval)
                else:
                    pass

                try:
                    self.check_for_stopped_jobs()
                    self.check_for_finished_jobs()
                    self.check_for_new_jobs()

                    status = self.status()

                    status_file = self.options["status_file"]
                    if status_file != "none":
                        status_file = Path(status_file).expanduser()
                        with open(status_file, "w") as fd:
                            json.dump(status, fd, indent=4)
                except Exception as e:
                    print(f"Error: {e}\n\n{traceback.format_exc()}")
        logger.info("Stopping the JobServer and closing the database.")
        self._db.close()
        logger.info("Good bye!")

    def _reattach_local_jobs(self):
        """On startup, find any locally-run jobs already in progress.

        Runs unconditionally, regardless of whether this JobServer instance
        is currently SLURM-configured -- a 'running'/'kill' row with a real
        recorded ``pid`` is a genuine local job no matter the instance's
        current mode (e.g. it predates SLURM being enabled here), and must
        be checked before anything else gets a chance to treat it as some
        other kind of lost job. A row with no ``pid`` but a recorded
        ``slurm_job_id`` is deliberately left alone here -- that is
        ``_reattach_slurm_jobs``'s job, called separately, only when SLURM
        is actually configured (checking it needs a real backend to poll).

        Also picks up ``kill``-status rows, not just ``running`` ones: a
        job whose kill was requested right before a restart still has a
        real process that needs to actually be terminated, not just
        forgotten about.
        """
        for row in self.db.execute(
            "SELECT id, status, path, json_extract(parameters, '$.pid'),"
            "       json_extract(parameters, '$.slurm_job_id')"
            "  FROM jobs"
            " WHERE status IN ('running', 'kill')"
        ):
            job_id, orig_status, wdir, pid, slurm_id = row

            if pid is None:
                if slurm_id is not None:
                    # A real SLURM submission with no local pid -- not this
                    # method's job to resolve.
                    continue
                finished = True
            else:
                finished = False
                try:
                    process = psutil.Process(pid=pid)
                except psutil.NoSuchProcess:
                    finished = True
                else:
                    if process.is_running():
                        self._jobs[job_id] = {
                            "mode": "local",
                            "pid": process.pid,
                            "process": process,
                            "wdir": wdir,
                        }
                        self._times[job_id] = {}
                    else:
                        finished = True

            if finished:
                if orig_status == "kill":
                    final_status = "killed"
                else:
                    # Trust job_data.json's own verdict over assuming
                    # success just because the process ended -- the same
                    # principle applied everywhere else a job's outcome is
                    # determined (see _check_for_finished_jobs_local).
                    state = self._read_job_data_state(wdir)
                    final_status = state if state is not None else "finished"
                self.logger.info(f"Job {job_id} already {final_status} (pid={pid}).")
                if orig_status == "kill":
                    self.killed_jobs += 1
                elif final_status == "finished":
                    self.successful_jobs += 1
                else:
                    self.failed_jobs += 1
                try:
                    current_time = datetime.now(timezone.utc)
                    cursor = self.db.cursor()
                    cursor.execute(
                        "UPDATE jobs"
                        "   SET status = ?, finished = ?,"
                        "       parameters=json_remove(jobs.parameters, '$.pid')"
                        " WHERE id = ?",
                        (final_status, current_time, job_id),
                    )
                    self.db.commit()
                except Exception as e:
                    self.logger.warning(f"Could not update job {job_id}: {e}")
            else:
                self.previous_jobs += 1
                self.logger.info(f"Job {job_id} is still running (pid={pid}).")
                if orig_status == "kill":
                    self.logger.info(
                        f"Job {job_id} was marked for killing before restart; "
                        "will stop it on the next poll cycle."
                    )

    def _reattach_slurm_jobs(self):
        """On startup, find any SLURM-submitted jobs already in progress.

        Called separately from, and after, ``_reattach_local_jobs`` -- and
        only when this JobServer instance is actually SLURM-configured,
        since polling needs a real backend. A row with a recorded ``pid``
        is skipped entirely here: ``_reattach_local_jobs`` (called first,
        unconditionally) already resolved it one way or another -- either
        it's still tracked as a genuine local process, or it was just
        finalized -- and treating it as a lost SLURM submission here too
        would be wrong on both counts (double-tracking a live local job as
        also needing SLURM reconciliation, or trying to stage-out/resubmit
        a job that never went anywhere near SLURM in the first place).

        SLURM is the source of truth here, not any state JobServer itself
        remembered before restarting: look up each `running`-status job's
        last known `slurm_job_id` and ask SLURM. Still pending/running ->
        resume tracking it. Terminal, gone, or never even got a
        `slurm_job_id` recorded (JobServer died mid-submission, or it's a
        stray row from before SLURM was ever configured on this instance)
        -> the same reconciliation path as the steady-state case (trust
        job_data.json, else resubmit up to a cap, else give up) -- safe
        because flowcharts checkpoint completed steps and safely resume.

        Also picks up ``kill``-status rows, not just ``running`` ones -- a
        job whose kill was requested right before a restart still needs its
        real SLURM job cancelled, not silently resumed or, worse,
        resubmitted by the ordinary reconciliation path.
        """
        rows = list(
            self.db.execute(
                "SELECT id, status, path, json_extract(parameters, '$.pid'),"
                "       json_extract(parameters, '$.slurm_job_id'),"
                "       json_extract(parameters, '$.cmdline'),"
                "       json_extract(parameters, '$.resubmit_count'),"
                "       json_extract(parameters, '$.slurm')"
                "  FROM jobs"
                " WHERE status IN ('running', 'kill')"
            )
        )
        if not rows:
            return

        ids = [
            str(slurm_id)
            for _, _, _, pid, slurm_id, _, _, _ in rows
            if pid is None and slurm_id is not None
        ]
        statuses = self._slurm_backend.poll_many(ids) if ids else {}

        for (
            job_id,
            orig_status,
            wdir,
            pid,
            slurm_id,
            cmdline_json,
            resubmit_count,
            slurm_overrides_json,
        ) in rows:
            if pid is not None:
                # Already resolved by _reattach_local_jobs.
                continue

            resubmit_count = resubmit_count or 0
            cmdline = json.loads(cmdline_json) if cmdline_json else []
            slurm_overrides = (
                json.loads(slurm_overrides_json) if slurm_overrides_json else {}
            )
            # Only meaningful -- and only computed -- for a job that was
            # actually dispatched to SLURM before (a real slurm_job_id is
            # on record): there's nothing remote to reconcile for a row
            # that never went there, and computing/using it anyway is
            # exactly the bug this docstring warns about above.
            remote_wdir = (
                self._remote_wdir(wdir)
                if slurm_id is not None and self._slurm.transport == "ssh"
                else None
            )

            status = statuses.get(str(slurm_id)) if slurm_id is not None else None
            if status is not None and not status.is_terminal:
                self._jobs[job_id] = {
                    "mode": "slurm",
                    "slurm_job_id": str(slurm_id),
                    "wdir": wdir,
                    "remote_wdir": remote_wdir,
                    "cmdline": cmdline,
                    "resubmit_count": resubmit_count,
                    "slurm_overrides": slurm_overrides,
                }
                self._times[job_id] = {}
                self.previous_jobs += 1
                if orig_status == "kill":
                    self.logger.info(
                        f"Job {job_id} (slurm_job_id={slurm_id}) was marked "
                        "for killing before restart; will stop it on the "
                        "next poll cycle."
                    )
                else:
                    self.logger.info(
                        f"Job {job_id} (slurm_job_id={slurm_id}) is still "
                        f"{status.category}."
                    )
                continue

            if orig_status == "kill":
                self.logger.info(
                    f"Job {job_id} (slurm_job_id={slurm_id}) was marked for "
                    "killing before restart, and is already gone from "
                    "SLURM; finalizing as killed."
                )
                self.killed_jobs += 1
                self._finalize_job_status(job_id, "killed", expected_current="kill")
                continue

            if slurm_id is None:
                self.logger.warning(
                    f"Job {job_id} has no recorded slurm_job_id (JobServer "
                    "likely died mid-submission)."
                )
            self._finalize_or_resubmit_slurm_job(
                job_id,
                {
                    "wdir": wdir,
                    "remote_wdir": remote_wdir,
                    "cmdline": cmdline,
                    "slurm_overrides": slurm_overrides,
                },
                resubmit_count,
            )

    def start_job(self, job_id, wdir, cmdline="", slurm_overrides=None):
        """Run the given job, locally or via SLURM depending on whether
        this JobServer instance has a SLURM config.

        Parameters
        ----------
        job_id : integer
            The id of the job to run.
        slurm_overrides : dict or None
            A job's requested per-directive SLURM overrides (its
            ``parameters["slurm"]``), e.g. ``{"ntasks": 4, "mem": "40G"}``.
            Ignored in local mode. Validated against the SLURM section's
            ``limits`` before use -- see ``SlurmSection.merge_overrides``.

        Returns
        -------
        int or None
            The local pid, if run locally. ``None`` if run via SLURM --
            ``_start_job_slurm`` updates the datastore itself in that case,
            since it also needs to record the SLURM job id.
        """
        self.logger.info("Starting job {}".format(job_id))

        if self._slurm is not None:
            # _start_job_slurm builds the command itself, after staging
            # (transport=ssh) determines the *effective* working directory
            # -- _build_cmd needs that, not the local wdir, to be correct.
            self._start_job_slurm(
                job_id, wdir, cmdline, slurm_overrides=slurm_overrides or {}
            )
            return None

        cmd = self._build_cmd(job_id, wdir, cmdline)
        self.logger.debug(f"cmd for {job_id}: {cmd}")
        return self._start_job_local(job_id, wdir, cmd)

    def _build_cmd(self, job_id, wdir, cmdline):
        """Build the ``run_from_jobserver`` command line for a job -- the
        same command whether it's about to be run as a local subprocess or
        as the payload of a SLURM batch script.

        ``wdir`` should already be the *effective* working directory the
        job will actually run in -- the remote-staged path for a
        ``transport = ssh`` SLURM section, or the ordinary local path
        otherwise (see ``_start_job_slurm``, which stages before calling
        this)."""
        if self._slurm is not None and self._slurm.transport == "ssh":
            prefix = self._remote_exe_prefix()
        else:
            path = sys.executable
            if path is not None and path != "":
                exe = Path(path).parent / "run_from_jobserver"
            else:
                exe = shutil.which("run_from_jobserver")
            prefix = [str(exe)]

        cmd = prefix + [str(job_id), str(wdir), str(self.db_path)]

        # Check if in docker container. Only meaningful for local execution
        # (local mode, or an on-cluster SLURM JobServer) -- this inspects
        # *this* host, which is irrelevant for a transport=ssh section
        # running on a different host entirely.
        if self._slurm is None or self._slurm.transport != "ssh":
            cgroup = Path("/proc/self/cgroup")
            if (
                Path("/.dockerenv").is_file()
                or cgroup.is_file()
                and "docker" in cgroup.read_text()
            ):
                cmd.append("--executor")
                cmd.append("docker")

        # Environment variable for debug output
        if "SEAMM_LOG_LEVEL" in os.environ:
            cmd.append("--log-level")
            cmd.append(os.environ["SEAMM_LOG_LEVEL"])

        cmd.extend(cmdline)

        return cmd

    def _remote_exe_prefix(self):
        """The argv prefix that invokes ``run_from_jobserver`` on a
        ``transport = ssh`` section's remote host.

        ``_build_cmd``'s ordinary ``sys.executable``-derived path is
        meaningless there -- that's *this* host's conda env. Prefers an
        explicit ``remote_run_from_jobserver`` absolute path (mirrors how
        local mode already invokes it, no shell activation needed); falls
        back to ``conda run -n <remote_conda_env>`` if only that's set.
        """
        if self._slurm.remote_run_from_jobserver:
            return [self._slurm.remote_run_from_jobserver]
        if self._slurm.remote_conda_env:
            return [
                "conda",
                "run",
                "-n",
                self._slurm.remote_conda_env,
                "run_from_jobserver",
            ]
        raise RuntimeError(
            f"SLURM section '{self._slurm.name}' has transport=ssh but "
            "neither remote_run_from_jobserver nor remote_conda_env is set "
            "-- can't determine how to invoke run_from_jobserver on "
            f"{self._slurm.host}."
        )

    def _start_job_local(self, job_id, wdir, cmd):
        """Run a job as a local subprocess (today's, pre-SLURM, behavior)."""
        # Create a copy of the current environment with job-specific variables
        env = os.environ.copy()
        env["SEAMM_JOB_ID"] = str(job_id)
        env["SEAMM_JOBSERVER"] = self.options["name"]

        process = psutil.Popen(
            cmd,
            cwd=wdir,
            env=env,  # Pass the job-specific environment
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._jobs[job_id] = {
            "mode": "local",
            "pid": process.pid,
            "process": process,
            "wdir": wdir,
        }
        self._times[job_id] = {}
        self.logger.debug(f"   process = {process}")

        self.total_jobs += 1

        return process.pid

    def _remote_wdir(self, wdir):
        """The remote scratch path a ``transport = ssh`` section's job
        should run in, derived from its local working directory's name and
        the section's ``remote_root``. Deterministic across resubmits --
        job directory names (``Job_NNNNNN``) are unique across the whole
        datastore, not just within a project, so no collision risk."""
        if not self._slurm.remote_root:
            raise RuntimeError(
                f"SLURM section '{self._slurm.name}' has transport=ssh but "
                "no remote_root is set -- can't determine where to stage "
                f"jobs on {self._slurm.host}."
            )
        return str(PurePosixPath(self._slurm.remote_root) / Path(wdir).name)

    def _start_job_slurm(
        self, job_id, wdir, cmdline, resubmit_count=0, slurm_overrides=None
    ):
        """Submit a job to SLURM as a whole-flowchart batch job.

        For a ``transport = ssh`` section, ``wdir`` (this job's ordinary
        local working directory) is first staged out to a remote scratch
        directory via ``self._stager`` (``RsyncStager``) -- everything from
        there on (the ``--chdir`` directive, the command line baked into
        the sbatch payload) uses that *remote* path instead, since the
        remote host has no access to ``wdir`` itself. For
        ``transport = local`` (every case validated so far), staging is a
        no-op and the effective/local paths are the same.

        No conda activation is needed in the script for local-mode
        directives -- ``cmd[0]`` is already the full, absolute path to
        ``run_from_jobserver`` inside the active environment (see
        ``_build_cmd``), and SLURM inherits the submitting shell's
        environment (PATH included) by default, so the interpreter is
        found the same way it would be run directly. Confirmed working
        end-to-end during Phase 0/1 groundwork. ssh-transport instead uses
        ``_remote_exe_prefix``, since the local interpreter path is
        meaningless on the remote host.

        ``slurm_overrides`` (a job's own ``parameters["slurm"]``, e.g.
        ``{"ntasks": 4}``) is validated against the section's ``limits`` by
        ``merge_overrides`` -- raises ``ValueError`` for anything not
        overridable or out of bounds, which propagates up to
        ``check_for_new_jobs()``'s existing try/except and is handled the
        same as any other startup error (job marked ``startup error``).
        Never trusts that a caller (e.g. a web UI) already validated this.
        """
        slurm_overrides = slurm_overrides or {}

        remote_wdir = None
        effective_wdir = wdir
        if self._slurm.transport == "ssh":
            remote_wdir = self._remote_wdir(wdir)
            effective_wdir = self._stager.stage_in(wdir, remote_wdir)

        cmd = self._build_cmd(job_id, effective_wdir, cmdline)
        quoted_cmd = " ".join(shlex.quote(c) for c in cmd)
        payload = (
            f"export SEAMM_JOB_ID={shlex.quote(str(job_id))}\n"
            f"export SEAMM_JOBSERVER={shlex.quote(self.options['name'])}\n"
            f"{quoted_cmd}\n"
        )

        directives = self._slurm.merge_overrides(slurm_overrides)
        directives.setdefault("job_name", f"seamm-{job_id}")
        directives.setdefault("chdir", str(effective_wdir))
        script = build_script(directives, payload)

        # Best-effort: keep a copy in the job's own *local* directory for
        # debugging, alongside job_data.json/references.db, regardless of
        # transport. Not required for submission itself -- that goes over
        # sbatch's stdin.
        try:
            (Path(wdir) / "slurm_submit.sh").write_text(script)
        except OSError as e:
            self.logger.warning(f"Could not write slurm_submit.sh for {job_id}: {e}")

        slurm_job_id = self._slurm_backend.submit(script)
        self.logger.info(
            f"Job {job_id}: submitted to SLURM as {slurm_job_id} "
            f"(attempt {resubmit_count + 1})."
        )

        current_time = datetime.now(timezone.utc)
        cursor = self.db.cursor()
        cursor.execute(
            "UPDATE jobs"
            "   SET status = 'running', started = ?,"
            "       parameters = json_set(json_set(jobs.parameters,"
            "         '$.slurm_job_id', ?), '$.resubmit_count', ?)"
            " WHERE id = ?",
            (current_time, slurm_job_id, resubmit_count, job_id),
        )
        self.db.commit()

        self._jobs[job_id] = {
            "mode": "slurm",
            "slurm_job_id": slurm_job_id,
            "wdir": wdir,
            "remote_wdir": remote_wdir,
            "cmdline": cmdline,
            "resubmit_count": resubmit_count,
            "slurm_overrides": slurm_overrides,
        }
        self._times[job_id] = {}
        self.total_jobs += 1

        return slurm_job_id

    def status(self):
        """Get the current load, etc."""
        status = {}
        t_now = datetime.now()
        status["time"] = f"{t_now:%H:%M:%S}"
        status["previous jobs"] = self.previous_jobs
        status["total jobs"] = self.total_jobs
        status["successful jobs"] = self.successful_jobs
        status["failed jobs"] = self.failed_jobs
        status["ended jobs"] = self.ended_jobs
        status["killed jobs"] = self.killed_jobs

        # Cpu usage on machine
        times = psutil.cpu_times()
        status["machine user time"] = round(times.user, 1)
        status["machine system time"] = round(times.system, 1)
        status["machine idle time"] = round(times.idle, 1)
        status["machine % cpu"] = round(psutil.cpu_percent(interval=None), 1)
        memory = psutil.virtual_memory()
        total = humanize(memory.total)
        available = humanize(memory.available)
        pct = 100 * memory.available / memory.total
        status["available memory"] = available
        status["total memory"] = total
        status["memory % used"] = round(pct, 1)

        # The jobserver itself
        job_id = "JobServer"
        t = self._times[job_id]
        process = psutil.Process()
        pid = process.pid
        if pid not in t:
            t[pid] = {
                "user": 0.0,
                "system": 0.0,
                "time": time.perf_counter(),
            }
        else:
            tpid = t[pid]
            if process.is_running():
                # Still running!
                with process.oneshot():
                    cpu = process.cpu_times()
                    memory = process.memory_info()
                    memory_percent = process.memory_percent()

                memory_percent = float(memory_percent)
                user = float(cpu.user)
                system = float(cpu.system)
                rss = humanize(memory.rss)

                t1 = time.perf_counter()
                delta_t = t1 - tpid["time"]
                pct_user = (user - tpid["user"]) / delta_t * 100.0
                pct_system = (system - tpid["system"]) / delta_t * 100.0
                cpu_percent = pct_user + pct_system
                cpu = user + system

                tpid["time"] = t1
                tpid["user"] = user
                tpid["system"] = system

                status["JobServer"] = {
                    "cpu %": round(cpu_percent, 1),
                    "cpu time": round(cpu, 1),
                    "resident memory": rss,
                    "memory %": round(memory_percent, 1),
                }
        js = status["Jobs"] = {}
        for job_id, data in self._jobs.items():
            if data.get("mode") == "slurm":
                # No local process to inspect -- report what SLURM knows
                # instead of psutil cpu/memory stats.
                js[job_id] = {
                    "slurm_job_id": data["slurm_job_id"],
                    "resubmit_count": data.get("resubmit_count", 0),
                }
                continue
            pid = data["pid"]
            if job_id not in self._times:
                self._times[job_id] = {}
            t = self._times[job_id]
            if pid not in t:
                t[pid] = {
                    "user": 0.0,
                    "system": 0.0,
                    "time": time.perf_counter(),
                }
            else:
                tpid = t[pid]
                process = data["process"]
                try:
                    if process.is_running():
                        # Still running!
                        with process.oneshot():
                            cpu = process.cpu_times()
                            memory = process.memory_info()
                            memory_percent = process.memory_percent()

                        memory_percent = float(memory_percent)
                        user = float(cpu.user)
                        system = float(cpu.system)
                        rss = humanize(memory.rss)

                        t1 = time.perf_counter()
                        delta_t = t1 - tpid["time"]
                        pct_user = (user - tpid["user"]) / delta_t * 100.0
                        pct_system = (system - tpid["system"]) / delta_t * 100.0
                        cpu_percent = pct_user + pct_system
                        cpu = user + system

                        tpid["time"] = t1
                        tpid["user"] = user
                        tpid["system"] = system

                        js[job_id] = {
                            "cpu %": round(cpu_percent, 1),
                            "cpu time": round(cpu, 1),
                            "resident memory": rss,
                            "memory %": round(memory_percent, 1),
                        }
                        sub = js[job_id]["sub processes"] = {}
                        for p in process.children(recursive=True):
                            with p.oneshot():
                                pid = p.pid
                                name = p.name()
                                cpu = p.cpu_times()
                                memory = p.memory_info()
                                memory_percent = p.memory_percent()
                            if pid not in t:
                                t[pid] = {
                                    "user": 0.0,
                                    "system": 0.0,
                                    "time": time.perf_counter(),
                                }
                            else:
                                tpid = t[pid]
                                memory_percent = float(memory_percent)
                                user = float(cpu.user)
                                system = float(cpu.system)
                                rss = humanize(memory.rss)

                                t1 = time.perf_counter()
                                delta_t = t1 - tpid["time"]
                                pct_user = (user - tpid["user"]) / delta_t * 100.0
                                pct_system = (system - tpid["system"]) / delta_t * 100.0
                                cpu_percent = pct_user + pct_system
                                cpu = user + system

                                tpid["time"] = t1
                                tpid["user"] = user
                                tpid["system"] = system
                                sub[pid] = {
                                    "name": name,
                                    "cpu %": round(cpu_percent, 1),
                                    "cpu time": round(cpu, 1),
                                    "resident memory": rss,
                                    "memory %": round(memory_percent, 1),
                                }
                except psutil.NoSuchProcess:
                    pass
                except Exception as e:
                    self.logger.warning(
                        f"Warning getting status of {job_id}: {e}\n\n"
                        f"{traceback.format_exc()}"
                    )
        return status


if __name__ == "__main__":
    run()
