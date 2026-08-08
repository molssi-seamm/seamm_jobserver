# -*- coding: utf-8 -*-

"""Tests for the SLURM-submission path added to seamm_jobserver.JobServer.

Uses a real (temp-file) sqlite datastore with a minimal `jobs` table -- the
same statements JobServer itself issues -- and a FakeSlurmBackend standing
in for seamm_slurm, so these tests never touch a real SLURM installation.
`seamm_slurm`'s own test suite covers the backend/script-building logic in
isolation; these tests cover how JobServer *uses* it.
"""

import json
import sqlite3
from unittest import mock

import pytest

from seamm_jobserver.jobserver import JobServer
from seamm_slurm.config import FieldLimits, SlurmSection
from seamm_slurm.stage import StageError
from seamm_slurm.status import JobStatus


class FakeSlurmBackend:
    """A stand-in for seamm_slurm's SlurmBackend, scriptable per-test."""

    def __init__(self):
        self.submitted = []  # list of script text, in submission order
        self.next_job_id = 100
        self.statuses = {}  # slurm_job_id -> JobStatus, consulted by poll_many
        self.cancelled = []  # slurm_job_ids passed to cancel(), in order

    def submit(self, script, *, job_name=None):
        job_id = str(self.next_job_id)
        self.next_job_id += 1
        self.submitted.append(script)
        return job_id

    def poll_many(self, job_ids):
        return {j: self.statuses[j] for j in job_ids if j in self.statuses}

    def cancel(self, job_id):
        self.cancelled.append(job_id)


class FakeStager:
    """A stand-in for seamm_slurm's RsyncStager/LocalStager, scriptable
    per-test -- never touches real ssh/rsync."""

    def __init__(self):
        self.staged_in = []  # (local_wdir, remote_wdir), in call order
        self.staged_out = []  # (remote_wdir, local_wdir), in call order
        self.fail_stage_out_times = 0

    def stage_in(self, local_wdir, remote_wdir):
        self.staged_in.append((local_wdir, remote_wdir))
        return remote_wdir

    def stage_out(self, remote_wdir, local_wdir):
        if self.fail_stage_out_times > 0:
            self.fail_stage_out_times -= 1
            raise StageError("transient failure")
        self.staged_out.append((remote_wdir, local_wdir))


class FakeProcess:
    """A stand-in for the psutil.Process a local-mode job tracks."""

    def __init__(self, running=False, returncode=0):
        self.terminated = False
        self.killed = False
        self.pid = 4242
        self._running = running
        self.returncode = returncode

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        pass

    def kill(self):
        self.killed = True

    def is_running(self):
        return self._running

    def status(self):
        return "running" if self._running else "terminated"


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "seamm.db"
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE jobs ("
        " id INTEGER PRIMARY KEY,"
        " path TEXT,"
        " status TEXT,"
        " started TEXT,"
        " finished TEXT,"
        " parameters TEXT"
        ")"
    )
    db.commit()
    db.close()
    return path


def insert_job(db_path, job_id, status, path, cmdline=None, extra_params=None):
    params = dict(extra_params or {})
    params["cmdline"] = cmdline or []
    db = sqlite3.connect(db_path)
    db.execute(
        "INSERT INTO jobs (id, path, status, parameters) VALUES (?, ?, ?, ?)",
        (job_id, path, status, json.dumps(params)),
    )
    db.commit()
    db.close()


def get_job(db_path, job_id):
    db = sqlite3.connect(db_path)
    row = db.execute(
        "SELECT status, parameters FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    db.close()
    status, parameters = row
    return status, json.loads(parameters)


def make_jobserver(db_path, wdir, max_concurrent_jobs=20, max_resubmits=3, limits=None):
    js = JobServer()
    js.options = {"name": "molssi10"}
    js.db_path = str(db_path)
    js._slurm = SlurmSection(
        name="molssi10",
        transport="local",
        host=None,
        directives={"partition": "batch"},
        max_concurrent_jobs=max_concurrent_jobs,
        max_resubmits=max_resubmits,
        limits=limits or {},
    )
    js._slurm_backend = FakeSlurmBackend()
    return js


def make_local_jobserver(db_path):
    """A JobServer with no SLURM config at all -- local-subprocess mode."""
    js = JobServer()
    js.options = {"name": "molssi10"}
    js.db_path = str(db_path)
    return js


def make_ssh_jobserver(
    db_path,
    max_concurrent_jobs=20,
    max_resubmits=3,
    remote_root="/remote/scratch",
    remote_run_from_jobserver="/remote/bin/run_from_jobserver",
    remote_conda_env=None,
):
    """A JobServer with a transport=ssh SLURM section -- no shared
    filesystem with 'the remote host', standing in for the Phase 8 case
    (e.g. a Mac JobServer dispatching to MolSSI10). FakeStager stands in
    for RsyncStager so no real ssh/rsync ever runs."""
    js = JobServer()
    js.options = {"name": "molssi10"}
    js.db_path = str(db_path)
    js._slurm = SlurmSection(
        name="molssi10",
        transport="ssh",
        host="molssi10",
        directives={"partition": "batch"},
        max_concurrent_jobs=max_concurrent_jobs,
        max_resubmits=max_resubmits,
        remote_root=remote_root,
        remote_conda_env=remote_conda_env,
        remote_run_from_jobserver=remote_run_from_jobserver,
    )
    js._slurm_backend = FakeSlurmBackend()
    js._stager = FakeStager()
    return js


# ---- start_job / _start_job_slurm ---------------------------------------


def test_start_job_slurm_writes_slurm_job_id_and_status(db_path, tmp_path):
    wdir = tmp_path / "Job_001"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))

    js = make_jobserver(db_path, wdir)
    result = js.start_job(1, str(wdir), [])

    assert result is None  # _start_job_slurm already wrote the DB itself
    status, params = get_job(db_path, 1)
    assert status == "running"
    assert params["slurm_job_id"] == "100"
    assert params["resubmit_count"] == 0
    assert 1 in js._jobs
    assert js._jobs[1]["mode"] == "slurm"


def test_start_job_slurm_script_has_directives_and_no_conda_activate(db_path, tmp_path):
    wdir = tmp_path / "Job_002"
    wdir.mkdir()
    insert_job(db_path, 2, "submitted", str(wdir))

    js = make_jobserver(db_path, wdir)
    js.start_job(2, str(wdir), [])

    script = js._slurm_backend.submitted[0]
    assert script.startswith("#!/bin/bash")
    assert "#SBATCH --partition=batch" in script
    assert f"#SBATCH --chdir={wdir}" in script
    assert "#SBATCH --job-name=seamm-2" in script
    assert "export SEAMM_JOB_ID=2" in script
    assert "export SEAMM_JOBSERVER=molssi10" in script
    assert "conda activate" not in script
    assert "run_from_jobserver" in script


def test_start_job_slurm_writes_debug_script_to_wdir(db_path, tmp_path):
    wdir = tmp_path / "Job_003"
    wdir.mkdir()
    insert_job(db_path, 3, "submitted", str(wdir))

    js = make_jobserver(db_path, wdir)
    js.start_job(3, str(wdir), [])

    assert (wdir / "slurm_submit.sh").exists()


# ---- check_for_new_jobs: concurrency cap ---------------------------------


def test_check_for_new_jobs_respects_concurrency_cap(db_path, tmp_path):
    for i in range(1, 4):
        wdir = tmp_path / f"Job_{i}"
        wdir.mkdir()
        insert_job(db_path, i, "submitted", str(wdir))

    js = make_jobserver(db_path, tmp_path, max_concurrent_jobs=2)
    js.check_for_new_jobs()

    assert len(js._jobs) == 2
    statuses = [get_job(db_path, i)[0] for i in range(1, 4)]
    assert statuses.count("running") == 2
    assert statuses.count("submitted") == 1


def test_check_for_new_jobs_no_op_when_at_cap(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))

    js = make_jobserver(db_path, tmp_path, max_concurrent_jobs=1)
    js._jobs[999] = {"mode": "slurm"}  # pretend a slot is already taken

    js.check_for_new_jobs()

    assert js._slurm_backend.submitted == []
    assert get_job(db_path, 1)[0] == "submitted"


# ---- check_for_finished_jobs (SLURM) -------------------------------------


def test_finished_jobs_completed_finalizes_from_job_data_json(db_path, tmp_path):
    # The common case: the JobServer, not the job's own process, is the
    # one that writes the datastore's terminal status (Phase 8) -- it
    # trusts the job_data.json the flowchart wrote in its finally block.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "finished"}\n')

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="COMPLETED", category="completed", exit_code="0:0"
    )

    js.check_for_finished_jobs()

    assert 1 not in js._jobs
    assert get_job(db_path, 1)[0] == "finished"
    assert js.successful_jobs == 1
    assert js._slurm_backend.submitted == []  # no resubmit needed


def test_finished_jobs_already_finalized_status_left_alone(db_path, tmp_path):
    # Idempotency guard: if the datastore row is no longer "running" by
    # the time this runs (e.g. another poll cycle or check_for_stopped_jobs
    # already finalized it), don't re-finalize or double-count it.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "finished", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="COMPLETED", category="completed", exit_code="0:0"
    )

    js.check_for_finished_jobs()

    assert 1 not in js._jobs
    assert get_job(db_path, 1)[0] == "finished"
    assert js.successful_jobs == 0
    assert js.failed_jobs == 0


def test_finished_jobs_completed_but_still_running_trusts_job_data_json(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "error"}\n')

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="COMPLETED", category="completed", exit_code="0:0"
    )

    js.check_for_finished_jobs()

    assert get_job(db_path, 1)[0] == "error"
    assert js._slurm_backend.submitted == []  # trusted job_data.json, no resubmit


def test_finished_jobs_failed_and_no_job_data_json_resubmits(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver", "1", str(wdir)],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="NODE_FAIL", category="failed"
    )

    js.check_for_finished_jobs()

    assert len(js._slurm_backend.submitted) == 1  # resubmitted
    status, params = get_job(db_path, 1)
    assert status == "running"
    assert params["resubmit_count"] == 1
    assert 1 in js._jobs  # re-tracked under the new slurm_job_id
    assert js._jobs[1]["slurm_job_id"] == "100"


def test_finished_jobs_gives_up_after_max_resubmits(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(db_path, wdir, max_resubmits=2)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 2,  # already at the cap
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="FAILED", category="failed"
    )

    js.check_for_finished_jobs()

    assert js._slurm_backend.submitted == []  # no further resubmit
    assert get_job(db_path, 1)[0] == "error"
    assert 1 not in js._jobs


def test_finished_jobs_missing_from_slurm_is_treated_as_lost(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    # No entry in js._slurm_backend.statuses for "42" -- poll_many omits it.

    js.check_for_finished_jobs()

    assert len(js._slurm_backend.submitted) == 1  # resubmitted
    assert 1 in js._jobs  # re-tracked under the new slurm_job_id
    assert js._jobs[1]["slurm_job_id"] == "100"


def test_still_running_jobs_are_left_alone(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="RUNNING", category="running"
    )

    js.check_for_finished_jobs()

    assert 1 in js._jobs
    assert js._slurm_backend.submitted == []


def test_check_for_finished_jobs_no_op_when_no_jobs_tracked(db_path, tmp_path):
    js = make_jobserver(db_path, tmp_path)
    js.check_for_finished_jobs()  # should not raise


# ---- check_for_finished_jobs (local) --------------------------------------


def test_finished_jobs_local_finalizes_from_job_data_json(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "finished"}\n')

    js = make_local_jobserver(db_path)
    process = FakeProcess(running=False, returncode=0)
    js._jobs[1] = {
        "mode": "local",
        "pid": process.pid,
        "process": process,
        "wdir": str(wdir),
    }
    js._times[1] = {}

    js.check_for_finished_jobs()

    assert 1 not in js._jobs
    assert get_job(db_path, 1)[0] == "finished"
    assert js.successful_jobs == 1


def test_finished_jobs_local_job_data_json_wins_over_exit_code(db_path, tmp_path):
    # A flowchart can exit 0 at the OS level while still having recorded
    # its own run as an error -- job_data.json is the more authoritative
    # source, not the process exit code.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "error"}\n')

    js = make_local_jobserver(db_path)
    process = FakeProcess(running=False, returncode=0)
    js._jobs[1] = {
        "mode": "local",
        "pid": process.pid,
        "process": process,
        "wdir": str(wdir),
    }
    js._times[1] = {}

    js.check_for_finished_jobs()

    assert get_job(db_path, 1)[0] == "error"
    assert js.failed_jobs == 1
    assert js.successful_jobs == 0


def test_finished_jobs_local_no_job_data_json_falls_back_to_exit_code(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    # No job_data.json written -- a hard crash before the finally block ran.

    js = make_local_jobserver(db_path)
    process = FakeProcess(running=False, returncode=1)
    js._jobs[1] = {
        "mode": "local",
        "pid": process.pid,
        "process": process,
        "wdir": str(wdir),
    }
    js._times[1] = {}

    js.check_for_finished_jobs()

    assert get_job(db_path, 1)[0] == "error"
    assert js.failed_jobs == 1


def test_finished_jobs_local_still_running_is_left_alone(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_local_jobserver(db_path)
    process = FakeProcess(running=True)
    js._jobs[1] = {
        "mode": "local",
        "pid": process.pid,
        "process": process,
        "wdir": str(wdir),
    }
    js._times[1] = {}

    js.check_for_finished_jobs()

    assert 1 in js._jobs
    assert get_job(db_path, 1)[0] == "running"


# ---- transport=ssh: stage_in/stage_out (Phase 8) --------------------------


def test_remote_wdir_derivation(db_path, tmp_path):
    js = make_ssh_jobserver(db_path, remote_root="/remote/scratch")
    assert js._remote_wdir("/local/Jobs/projects/x/Job_000005") == (
        "/remote/scratch/Job_000005"
    )


def test_remote_wdir_without_remote_root_raises(db_path, tmp_path):
    js = make_ssh_jobserver(db_path, remote_root=None)
    with pytest.raises(RuntimeError, match="remote_root"):
        js._remote_wdir("/local/Job_1")


def test_start_job_ssh_stages_in_and_uses_remote_wdir(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))

    js = make_ssh_jobserver(db_path, remote_root="/remote/scratch")
    js.start_job(1, str(wdir), [])

    assert js._stager.staged_in == [(str(wdir), "/remote/scratch/Job_1")]

    script = js._slurm_backend.submitted[0]
    assert "#SBATCH --chdir=/remote/scratch/Job_1" in script
    assert "/remote/bin/run_from_jobserver" in script
    # The local wdir must not leak into the remote command line.
    assert str(wdir) not in script

    # The debug copy still goes to the *local* directory, for Paul to see.
    assert (wdir / "slurm_submit.sh").exists()


def test_start_job_ssh_falls_back_to_conda_run(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))

    js = make_ssh_jobserver(
        db_path, remote_run_from_jobserver=None, remote_conda_env="seamm"
    )
    js.start_job(1, str(wdir), [])

    script = js._slurm_backend.submitted[0]
    assert "conda run -n seamm run_from_jobserver" in script


def test_start_job_ssh_without_remote_exe_config_raises(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))

    js = make_ssh_jobserver(
        db_path, remote_run_from_jobserver=None, remote_conda_env=None
    )
    with pytest.raises(RuntimeError, match="remote_run_from_jobserver"):
        js.start_job(1, str(wdir), [])


def test_finished_jobs_ssh_stages_out_before_reading_job_data_json(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    # Stands in for what a real stage_out would have pulled back.
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "finished"}\n')

    js = make_ssh_jobserver(db_path)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "remote_wdir": "/remote/scratch/Job_1",
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="COMPLETED", category="completed", exit_code="0:0"
    )

    js.check_for_finished_jobs()

    assert js._stager.staged_out == [("/remote/scratch/Job_1", str(wdir))]
    assert get_job(db_path, 1)[0] == "finished"
    assert js.successful_jobs == 1
    assert 1 not in js._jobs


def test_finished_jobs_ssh_stage_out_failure_retries_next_cycle(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "finished"}\n')

    js = make_ssh_jobserver(db_path)
    js._stager.fail_stage_out_times = 1
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "remote_wdir": "/remote/scratch/Job_1",
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="COMPLETED", category="completed", exit_code="0:0"
    )

    js.check_for_finished_jobs()

    # First attempt failed to stage out -- not finalized, not resubmitted,
    # still tracked so the next poll cycle retries.
    assert get_job(db_path, 1)[0] == "running"
    assert 1 in js._jobs
    assert js._slurm_backend.submitted == []

    js.check_for_finished_jobs()

    assert get_job(db_path, 1)[0] == "finished"
    assert 1 not in js._jobs


def test_reattach_ssh_still_running_job_recomputes_remote_wdir(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "running",
        str(wdir),
        extra_params={"slurm_job_id": "42", "resubmit_count": 0},
    )

    js = make_ssh_jobserver(db_path, remote_root="/remote/scratch")
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="PENDING", category="pending"
    )

    js._reattach_slurm_jobs()

    assert 1 in js._jobs
    assert js._jobs[1]["remote_wdir"] == "/remote/scratch/Job_1"


# ---- startup reattachment -------------------------------------------------


def test_reattach_still_running_job_is_retracked(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "running",
        str(wdir),
        extra_params={"slurm_job_id": "42", "resubmit_count": 0},
    )

    js = make_jobserver(db_path, wdir)
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="PENDING", category="pending"
    )

    js._reattach_slurm_jobs()

    assert 1 in js._jobs
    assert js._jobs[1]["slurm_job_id"] == "42"
    assert js.previous_jobs == 1


def test_reattach_terminal_job_resubmits(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "running",
        str(wdir),
        extra_params={"slurm_job_id": "42", "resubmit_count": 0},
    )

    js = make_jobserver(db_path, wdir)
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="TIMEOUT", category="failed"
    )

    js._reattach_slurm_jobs()

    assert len(js._slurm_backend.submitted) == 1
    status, params = get_job(db_path, 1)
    assert status == "running"
    assert params["slurm_job_id"] == "100"


def test_reattach_job_with_no_recorded_slurm_id_resubmits(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))  # no slurm_job_id at all

    js = make_jobserver(db_path, wdir)

    js._reattach_slurm_jobs()

    assert len(js._slurm_backend.submitted) == 1
    assert get_job(db_path, 1)[1]["slurm_job_id"] == "100"


def test_reattach_no_running_jobs_is_a_no_op(db_path, tmp_path):
    js = make_jobserver(db_path, tmp_path)
    js._reattach_slurm_jobs()  # should not raise, no SLURM calls needed
    assert js._jobs == {}


# ---- _read_job_data_state --------------------------------------------------
#
# Found via real end-to-end testing on MolSSI10 (Phase 3): seamm_exec's
# run_from_jobserver() exception handler used to write the header without a
# trailing newline ("!MolSSI job_data 1.0{...}", all on one line), unlike
# every other writer of this file format. A plain readline()-then-json.load()
# reader (what this method used to do, and what
# seamm_datastore.Job.parse_job_data also does) silently failed to parse that
# form, making _reconcile_stalled_job wrongly resubmit jobs that had already
# recorded their real outcome. Fixed at the source (seamm_exec) too, but this
# reader stays tolerant of both forms since old files may already exist.


def test_read_job_data_state_normal_header_with_newline(tmp_path):
    (tmp_path / "job_data.json").write_text(
        '!MolSSI job_data 1.0\n{\n   "state": "finished"\n}\n'
    )
    assert JobServer._read_job_data_state(tmp_path) == "finished"


def test_read_job_data_state_header_without_newline(tmp_path):
    (tmp_path / "job_data.json").write_text(
        '!MolSSI job_data 1.0{\n   "state": "error"\n}\n'
    )
    assert JobServer._read_job_data_state(tmp_path) == "error"


def test_read_job_data_state_missing_file(tmp_path):
    assert JobServer._read_job_data_state(tmp_path) is None


def test_read_job_data_state_unparseable_content(tmp_path):
    (tmp_path / "job_data.json").write_text("not json at all")
    assert JobServer._read_job_data_state(tmp_path) is None


# ---- per-job SLURM overrides (parameters["slurm"]) -----------------------


def test_start_job_applies_valid_override(db_path, tmp_path):
    wdir = tmp_path / "Job_001"
    wdir.mkdir()
    insert_job(
        db_path, 1, "submitted", str(wdir), extra_params={"slurm": {"ntasks": 4}}
    )

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js.check_for_new_jobs()

    assert get_job(db_path, 1)[0] == "running"
    script = js._slurm_backend.submitted[0]
    assert "#SBATCH --ntasks=4" in script
    assert js._jobs[1]["slurm_overrides"] == {"ntasks": 4}


def test_start_job_rejects_unauthorized_override(db_path, tmp_path):
    wdir = tmp_path / "Job_001"
    wdir.mkdir()
    insert_job(
        db_path, 1, "submitted", str(wdir), extra_params={"slurm": {"account": "x"}}
    )

    js = make_jobserver(db_path, wdir)  # no limits configured at all
    js.check_for_new_jobs()

    assert get_job(db_path, 1)[0] == "startup error"
    assert js._slurm_backend.submitted == []
    assert 1 not in js._jobs


def test_start_job_rejects_out_of_range_override(db_path, tmp_path):
    wdir = tmp_path / "Job_001"
    wdir.mkdir()
    insert_job(
        db_path, 1, "submitted", str(wdir), extra_params={"slurm": {"ntasks": 99}}
    )

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js.check_for_new_jobs()

    assert get_job(db_path, 1)[0] == "startup error"
    assert js._slurm_backend.submitted == []


def test_start_job_with_no_override_still_works(db_path, tmp_path):
    wdir = tmp_path / "Job_001"
    wdir.mkdir()
    insert_job(db_path, 1, "submitted", str(wdir))  # no "slurm" key at all

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js.check_for_new_jobs()

    assert get_job(db_path, 1)[0] == "running"
    assert js._jobs[1]["slurm_overrides"] == {}


def test_resubmit_preserves_original_override(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
        "slurm_overrides": {"ntasks": 3},
    }
    js._times[1] = {}
    # No entry in js._slurm_backend.statuses for "42" -- poll_many omits it,
    # so this looks like a lost job and should be resubmitted.

    js.check_for_finished_jobs()

    assert len(js._slurm_backend.submitted) == 1
    assert "#SBATCH --ntasks=3" in js._slurm_backend.submitted[0]
    assert js._jobs[1]["slurm_overrides"] == {"ntasks": 3}


def test_reattach_lost_job_resubmits_with_original_override(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "running",
        str(wdir),
        extra_params={
            "slurm_job_id": "42",
            "resubmit_count": 0,
            "slurm": {"ntasks": 5},
        },
    )

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="FAILED", category="failed"
    )

    js._reattach_slurm_jobs()

    assert len(js._slurm_backend.submitted) == 1
    assert "#SBATCH --ntasks=5" in js._slurm_backend.submitted[0]


def test_check_for_stopped_jobs_cancels_slurm_job_whose_row_was_deleted(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    # Note: no insert_job() -- the row was already deleted (e.g. via the
    # dashboard's delete endpoint), just like real usage.

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}

    js.check_for_stopped_jobs()

    assert js._slurm_backend.cancelled == ["42"]
    assert 1 not in js._jobs
    assert 1 not in js._times
    assert js.killed_jobs == 1


def test_check_for_stopped_jobs_leaves_untouched_jobs_alone(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "running", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}

    js.check_for_stopped_jobs()

    assert js._slurm_backend.cancelled == []
    assert 1 in js._jobs
    assert js.killed_jobs == 0


def test_check_for_stopped_jobs_kills_tracked_slurm_job_on_kill_status(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}

    js.check_for_stopped_jobs()

    assert js._slurm_backend.cancelled == ["42"]
    assert 1 not in js._jobs
    assert get_job(db_path, 1)[0] == "killed"


def test_check_for_stopped_jobs_kill_status_not_yet_started(db_path, tmp_path):
    # Killed before the JobServer ever picked it up -- nothing to cancel,
    # just finalize the status.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir))

    js = make_jobserver(db_path, wdir)

    js.check_for_stopped_jobs()

    assert js._slurm_backend.cancelled == []
    assert get_job(db_path, 1)[0] == "killed"


def test_check_for_stopped_jobs_runs_before_reconciliation_would_resubmit(
    db_path, tmp_path
):
    # Regression check: a killed job must not get treated as merely "lost"
    # by check_for_finished_jobs and resubmitted.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmdline": ["run_from_jobserver"],
        "resubmit_count": 0,
    }
    js._times[1] = {}
    # No entry in statuses["42"] -- poll_many would treat this as lost.

    js.check_for_stopped_jobs()
    js.check_for_finished_jobs()

    assert js._slurm_backend.submitted == []  # never resubmitted
    assert js._slurm_backend.cancelled == ["42"]
    assert get_job(db_path, 1)[0] == "killed"


def test_check_for_stopped_jobs_local_mode_deleted_row(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()

    js = make_local_jobserver(db_path)
    process = FakeProcess()
    js._jobs[1] = {"mode": "local", "pid": process.pid, "process": process}
    js._times[1] = {}

    js.check_for_stopped_jobs()

    assert process.terminated
    assert 1 not in js._jobs
    assert js.killed_jobs == 1


def test_check_for_stopped_jobs_local_mode_kill_status(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir))

    js = make_local_jobserver(db_path)
    process = FakeProcess()
    js._jobs[1] = {"mode": "local", "pid": process.pid, "process": process}
    js._times[1] = {}

    js.check_for_stopped_jobs()

    assert process.terminated
    assert get_job(db_path, 1)[0] == "killed"


# ---- kill-status reattachment on restart -----------------------------------


def test_reattach_kill_status_job_still_active_is_tracked_not_finalized(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "kill",
        str(wdir),
        extra_params={"slurm_job_id": "42", "resubmit_count": 0},
    )

    js = make_jobserver(db_path, wdir)
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="RUNNING", category="running"
    )

    js._reattach_slurm_jobs()

    assert 1 in js._jobs
    assert js._jobs[1]["slurm_job_id"] == "42"
    assert js._slurm_backend.cancelled == []  # not cancelled yet
    assert get_job(db_path, 1)[0] == "kill"  # still pending, next cycle kills it

    # The next ordinary poll cycle then actually stops it.
    js.check_for_stopped_jobs()
    assert js._slurm_backend.cancelled == ["42"]
    assert get_job(db_path, 1)[0] == "killed"


def test_reattach_kill_status_job_already_gone_finalizes_as_killed(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "kill",
        str(wdir),
        extra_params={"slurm_job_id": "42", "resubmit_count": 0},
    )

    js = make_jobserver(db_path, wdir)
    # No entry for "42" in statuses -- SLURM has no record of it any more.

    js._reattach_slurm_jobs()

    assert 1 not in js._jobs
    assert js._slurm_backend.submitted == []  # not resubmitted
    assert get_job(db_path, 1)[0] == "killed"


def test_reattach_local_kill_status_job_still_running_is_tracked(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir), extra_params={"pid": 999999})

    js = make_local_jobserver(db_path)

    class FakeRunningProcess:
        pid = 999999

        def is_running(self):
            return True

    with mock.patch("psutil.Process", return_value=FakeRunningProcess()):
        js._reattach_local_jobs()

    assert 1 in js._jobs
    assert get_job(db_path, 1)[0] == "kill"


def test_reattach_local_kill_status_job_already_dead_finalizes_as_killed(
    db_path, tmp_path
):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "kill", str(wdir))  # no pid at all -> already gone

    js = make_local_jobserver(db_path)
    js._reattach_local_jobs()

    assert 1 not in js._jobs
    assert get_job(db_path, 1)[0] == "killed"


def test_reattach_still_running_job_keeps_override(db_path, tmp_path):
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(
        db_path,
        1,
        "running",
        str(wdir),
        extra_params={
            "slurm_job_id": "42",
            "resubmit_count": 0,
            "slurm": {"ntasks": 5},
        },
    )

    js = make_jobserver(
        db_path, wdir, limits={"ntasks": FieldLimits(minimum="1", maximum="6")}
    )
    js._slurm_backend.statuses["42"] = JobStatus(
        job_id="42", state="RUNNING", category="running"
    )

    js._reattach_slurm_jobs()

    assert js._jobs[1]["slurm_overrides"] == {"ntasks": 5}
