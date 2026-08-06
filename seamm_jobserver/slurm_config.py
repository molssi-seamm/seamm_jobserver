# -*- coding: utf-8 -*-

"""Load a JobServer's ``<root>/<jobserver-name>.ini`` SLURM/queue config.

System/machine config, not a user preference -- lives at ``<root>``
(``~/SEAMM`` by default), alongside ``orca.ini``/``lammps.ini``/
``dashboards.ini``, not in ``~/.seamm.d/seamm.ini``. Named after the
JobServer instance (its ``--name``, default hostname) rather than a fixed
``slurm.ini``, since a JobServer may eventually route jobs to more than one
cluster/queue -- one section per cluster/queue target. See
``~/Work/SEAMM/jobserver-slurm-plan.md`` for the full design.
"""

import configparser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import seamm_slurm

# Section keys that describe JobServer-side behavior rather than a SLURM
# submission directive -- not forwarded to seamm_slurm.script.build_script.
_NON_DIRECTIVE_KEYS = {
    "type",
    "transport",
    "host",
    "max_concurrent_jobs",
    "max_resubmits",
    "default",
}


@dataclass
class SlurmSection:
    """One cluster/queue target from a ``<root>/<jobserver-name>.ini`` file."""

    name: str
    transport: str
    host: Optional[str]
    directives: dict = field(default_factory=dict)
    max_concurrent_jobs: int = 20
    max_resubmits: int = 3

    def build_backend(self):
        """Construct the ``seamm_slurm`` backend this section describes."""
        if self.transport == "local":
            return seamm_slurm.LocalSlurm()
        elif self.transport == "ssh":
            if not self.host:
                raise RuntimeError(
                    f"SLURM section '{self.name}' has transport=ssh but no " "host set"
                )
            return seamm_slurm.SshSlurm(self.host)
        else:
            raise RuntimeError(
                f"SLURM section '{self.name}' has unknown transport "
                f"'{self.transport}' (expected 'local' or 'ssh')"
            )


def load_slurm_config(root, jobserver_name, section=None):
    """Load ``<root>/<jobserver_name>.ini``, if it exists.

    Parameters
    ----------
    root : str or Path
        The SEAMM root directory (e.g. ``~/SEAMM``), same as every other
        per-code ``.ini`` file.
    jobserver_name : str
        This JobServer instance's ``--name`` (default: hostname).
    section : str or None
        Which section to use. If ``None``, uses ``[DEFAULT]``'s
        ``default =`` key, falling back to the sole section if there is
        exactly one and no ``default`` is set.

    Returns
    -------
    SlurmSection or None
        ``None`` means "no such file" -- the caller should fall back to
        running jobs as local subprocesses, exactly as if this feature did
        not exist.
    """
    ini_path = Path(root).expanduser() / f"{jobserver_name}.ini"
    if not ini_path.exists():
        return None

    config = configparser.ConfigParser(interpolation=None)
    config.read(ini_path)

    if section is None:
        section = config.defaults().get("default")
    if section is None:
        candidates = config.sections()
        if len(candidates) == 1:
            section = candidates[0]
        else:
            raise RuntimeError(
                f"{ini_path} has no [DEFAULT] default= and no single, "
                "unambiguous section to use."
            )
    if section not in config:
        raise RuntimeError(f"{ini_path} has no section '{section}'")

    items = dict(config.items(section))

    transport = items.get("transport", "local")
    host = items.get("host") or None
    max_concurrent_jobs = int(items.get("max_concurrent_jobs", 20))
    max_resubmits = int(items.get("max_resubmits", 3))

    directives = {
        k: v for k, v in items.items() if k not in _NON_DIRECTIVE_KEYS and v != ""
    }

    return SlurmSection(
        name=section,
        transport=transport,
        host=host,
        directives=directives,
        max_concurrent_jobs=max_concurrent_jobs,
        max_resubmits=max_resubmits,
    )
