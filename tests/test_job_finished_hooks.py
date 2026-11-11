"""The ``org.molssi.seamm.jobserver.job_finished`` hooks: plug-ins' callables
run, in the background, once a job has ended -- e.g. the thermochemistry
fragment store's harvest of jobs that ran on a cluster."""

import logging
from types import SimpleNamespace

from tests.test_jobserver_slurm import (  # noqa: F401
    FakeProcess,
    db_path,
    get_job,
    insert_job,
    make_local_jobserver,
)


def _finish_local(js, job_id, wdir):
    process = FakeProcess(running=False, returncode=0)
    js._jobs[job_id] = {
        "mode": "local",
        "pid": process.pid,
        "process": process,
        "wdir": str(wdir),
    }
    js._times[job_id] = {}
    js.check_for_finished_jobs()


def _drain(js):
    executor = getattr(js, "_hook_executor", None)
    if executor is not None:
        executor.shutdown(wait=True)


def _job(tmp_path, db_path, job_id=1, status="running"):  # noqa: F811
    wdir = tmp_path / f"Job_{job_id}"
    wdir.mkdir()
    insert_job(db_path, job_id, status, str(wdir))
    (wdir / "job_data.json").write_text('!MolSSI job_data 1.0\n{"state": "finished"}\n')
    return wdir


def test_hooks_run_once_when_a_job_ends(db_path, tmp_path):  # noqa: F811
    calls = []
    js = make_local_jobserver(db_path)
    js._job_finished_hooks = [("record", lambda **kw: calls.append(kw))]
    wdir = _job(tmp_path, db_path)
    _finish_local(js, 1, wdir)
    _drain(js)
    assert get_job(db_path, 1)[0] == "finished"
    (call,) = calls
    assert call["job_id"] == 1 and call["state"] == "finished"
    assert call["wdir"] == wdir and call["logger"] is js.logger
    # Finalizing again (another poll cycle) changes nothing and runs nothing
    js._finalize_job_status(1, "finished")
    _drain(js)
    assert len(calls) == 1


def test_a_failing_hook_does_not_disturb_the_job(db_path, tmp_path, caplog):  # noqa
    def boom(**kw):
        raise RuntimeError("no store here")

    js = make_local_jobserver(db_path)
    js.logger = logging.getLogger("test-hooks")
    js._job_finished_hooks = [("boom", boom)]
    wdir = _job(tmp_path, db_path)
    with caplog.at_level(logging.WARNING, logger="test-hooks"):
        _finish_local(js, 1, wdir)
        _drain(js)
    assert get_job(db_path, 1)[0] == "finished"
    assert "job_finished hook 'boom' failed: no store here" in caplog.text


def test_an_untracked_job_gets_its_directory_from_the_datastore(
    db_path, tmp_path  # noqa: F811
):
    """A job killed before the JobServer tracked it (e.g. after a restart)."""
    calls = []
    js = make_local_jobserver(db_path)
    js._job_finished_hooks = [("record", lambda **kw: calls.append(kw))]
    wdir = _job(tmp_path, db_path, job_id=7, status="kill")
    js._finalize_job_status(7, "killed", expected_current="kill")
    _drain(js)
    assert get_job(db_path, 7)[0] == "killed"
    assert calls[0]["wdir"] == wdir and calls[0]["state"] == "killed"


def test_no_hooks_installed_nothing_runs(db_path, tmp_path):  # noqa: F811
    js = make_local_jobserver(db_path)
    js._job_finished_hooks = []
    _finish_local(js, 1, _job(tmp_path, db_path))
    assert getattr(js, "_hook_executor", None) is None
    assert get_job(db_path, 1)[0] == "finished"


def test_hooks_are_loaded_from_entry_points(db_path, monkeypatch, caplog):  # noqa
    def good(**kw):
        pass

    def bad_load():
        raise ImportError("missing dependency")

    entries = [
        SimpleNamespace(name="good", load=lambda: good),
        SimpleNamespace(name="bad", load=bad_load),
    ]
    seen = {}

    def fake_entry_points(group):
        seen["group"] = group
        return entries

    monkeypatch.setattr("importlib.metadata.entry_points", fake_entry_points)
    js = make_local_jobserver(db_path)
    js.logger = logging.getLogger("test-hooks")
    with caplog.at_level(logging.WARNING, logger="test-hooks"):
        hooks = js._finished_hooks()
    assert seen["group"] == "org.molssi.seamm.jobserver.job_finished"
    assert hooks == [("good", good)]
    assert "'bad' could not be loaded" in caplog.text
    # Loaded once
    entries.clear()
    assert js._finished_hooks() == [("good", good)]
