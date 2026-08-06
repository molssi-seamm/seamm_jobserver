# -*- coding: utf-8 -*-

"""Tests for seamm_jobserver.slurm_config.load_slurm_config."""

import pytest

import seamm_slurm
from seamm_jobserver.slurm_config import load_slurm_config


def test_missing_file_returns_none(tmp_path):
    assert load_slurm_config(tmp_path, "molssi10") is None


def test_single_section_no_default_key(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n" "transport = local\n" "partition = batch\n" "nodes = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section is not None
    assert section.name == "molssi10"
    assert section.transport == "local"
    assert section.host is None
    assert section.directives == {"partition": "batch", "nodes": "1"}
    assert section.max_concurrent_jobs == 20
    assert section.max_resubmits == 3


def test_default_key_picks_section_among_several(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[DEFAULT]\n"
        "default = chemai\n"
        "\n"
        "[molssi10]\n"
        "transport = local\n"
        "\n"
        "[chemai]\n"
        "transport = ssh\n"
        "host = seamm-chemai\n"
        "partition = ChemAI\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.name == "chemai"
    assert section.transport == "ssh"
    assert section.host == "seamm-chemai"
    assert section.directives == {"partition": "ChemAI"}


def test_multiple_sections_without_default_raises(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n[chemai]\ntransport = ssh\nhost = x\n"
    )
    with pytest.raises(RuntimeError, match="no single"):
        load_slurm_config(tmp_path, "molssi10")


def test_explicit_section_argument(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n[chemai]\ntransport = ssh\nhost = x\n"
    )
    section = load_slurm_config(tmp_path, "molssi10", section="chemai")
    assert section.name == "chemai"
    assert section.host == "x"


def test_unknown_section_name_raises(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    with pytest.raises(RuntimeError, match="no section 'nope'"):
        load_slurm_config(tmp_path, "molssi10", section="nope")


def test_blank_directive_values_are_dropped(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "partition = batch\n"
        "account =\n"
        "qos =\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.directives == {"partition": "batch"}


def test_max_concurrent_and_resubmits_overridable(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "max_concurrent_jobs = 5\n"
        "max_resubmits = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.max_concurrent_jobs == 5
    assert section.max_resubmits == 1


def test_build_backend_local():
    from seamm_jobserver.slurm_config import SlurmSection

    section = SlurmSection(name="x", transport="local", host=None)
    backend = section.build_backend()
    assert isinstance(backend, seamm_slurm.LocalSlurm)


def test_build_backend_ssh():
    from seamm_jobserver.slurm_config import SlurmSection

    section = SlurmSection(name="x", transport="ssh", host="molssi10")
    backend = section.build_backend()
    assert isinstance(backend, seamm_slurm.SshSlurm)
    assert backend.host == "molssi10"


def test_build_backend_ssh_without_host_raises():
    from seamm_jobserver.slurm_config import SlurmSection

    section = SlurmSection(name="x", transport="ssh", host=None)
    with pytest.raises(RuntimeError, match="no host"):
        section.build_backend()


def test_build_backend_unknown_transport_raises():
    from seamm_jobserver.slurm_config import SlurmSection

    section = SlurmSection(name="x", transport="pbs", host=None)
    with pytest.raises(RuntimeError, match="unknown transport"):
        section.build_backend()
