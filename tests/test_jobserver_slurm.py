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

import pytest

from seamm_jobserver.jobserver import JobServer
from seamm_jobserver.slurm_config import SlurmSection
from seamm_slurm.status import JobStatus


class FakeSlurmBackend:
    """A stand-in for seamm_slurm's SlurmBackend, scriptable per-test."""

    def __init__(self):
        self.submitted = []  # list of script text, in submission order
        self.next_job_id = 100
        self.statuses = {}  # slurm_job_id -> JobStatus, consulted by poll_many

    def submit(self, script, *, job_name=None):
        job_id = str(self.next_job_id)
        self.next_job_id += 1
        self.submitted.append(script)
        return job_id

    def poll_many(self, job_ids):
        return {j: self.statuses[j] for j in job_ids if j in self.statuses}

    def cancel(self, job_id):
        pass


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


def make_jobserver(db_path, wdir, max_concurrent_jobs=20, max_resubmits=3):
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
    )
    js._slurm_backend = FakeSlurmBackend()
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


def test_finished_jobs_completed_leaves_datastore_status_alone(db_path, tmp_path):
    # Mirrors run_from_jobserver's own direct write already having happened.
    wdir = tmp_path / "Job_1"
    wdir.mkdir()
    insert_job(db_path, 1, "finished", str(wdir))

    js = make_jobserver(db_path, wdir)
    js._jobs[1] = {
        "mode": "slurm",
        "slurm_job_id": "42",
        "wdir": str(wdir),
        "cmd": ["run_from_jobserver"],
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
        "cmd": ["run_from_jobserver"],
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
        "cmd": ["run_from_jobserver", "1", str(wdir)],
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
        "cmd": ["run_from_jobserver"],
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
        "cmd": ["run_from_jobserver"],
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
        "cmd": ["run_from_jobserver"],
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
