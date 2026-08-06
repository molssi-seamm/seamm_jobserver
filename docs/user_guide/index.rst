.. _user-guide:

**********
User Guide
**********

The JobServer polls a SEAMM datastore for jobs with status ``submitted`` and
runs them, one flowchart per job. By default it runs each job as a local
subprocess (``run_from_jobserver``) on the same host the JobServer itself is
running on -- this is unchanged and needs no configuration.

SLURM submission
=================

A JobServer instance can instead submit each job as a whole-flowchart SLURM
batch job. This is opt-in and entirely determined by whether a config file
exists -- there is no command-line flag.

Enabling it
-----------

Create ``<root>/<jobserver-name>.ini`` (e.g. ``~/SEAMM/molssi10.ini`` for a
JobServer started with ``--name molssi10``, which is the default -- the
JobServer's name defaults to the hostname it's running on). This is a
system/machine config file, like ``orca.ini`` or ``lammps.ini`` -- it lives
at ``<root>`` (the same directory as those), not in
``~/.seamm.d/seamm.ini``. If the file doesn't exist, the JobServer runs jobs
locally exactly as it always has.

The file has one section per cluster/queue this JobServer instance can
submit to (currently only one is actually used -- selecting among several is
not yet implemented, see below):

.. code-block:: ini

    [DEFAULT]
    # which section to use -- required if there's more than one section
    default = molssi10

    [molssi10]
    # transport: local (SLURM CLI on this host) | ssh (SLURM CLI on a
    # remote host, reached over passwordless SSH)
    transport = local
    # only used when transport = ssh
    # host = molssi10

    # SLURM submission directives -- each becomes an `#SBATCH --flag=value`
    # line. Any key not listed here is still passed through, converting
    # underscores to dashes (e.g. `gres = gpu:1` -> `#SBATCH --gres=gpu:1`).
    partition = batch
    account =
    qos =
    nodes = 1
    ntasks = 1
    time = 01:00:00
    mem =
    gpus =

    # JobServer behavior, not SLURM directives:
    # how many jobs this instance keeps outstanding in SLURM at once
    max_concurrent_jobs = 20
    # how many times to resubmit a job SLURM lost track of (see below)
    # before giving up and marking it "error"
    max_resubmits = 3

A blank value (``account =``) means "don't pass that directive at all" --
let SLURM's own defaults apply.

If there's exactly one section (besides ``[DEFAULT]``), it's used
automatically and ``default =`` is optional.

What happens
------------

When a job comes up for submission, the JobServer builds a script wrapping
the same ``run_from_jobserver`` command it would otherwise run directly --
no ``conda activate`` or other shell setup is needed, since the command
already uses the full path to the active Python environment's
``run_from_jobserver``, and SLURM inherits the submitting environment by
default. The script is submitted via ``sbatch``'s standard input (not
written to a file the target host needs to see -- this also works if
``transport = ssh`` and there's no shared filesystem with the cluster). A
copy is also saved to the job's own working directory, as
``slurm_submit.sh``, for reference.

The job's datastore row records the returned SLURM job ID (in its
``parameters`` JSON, alongside a ``resubmit_count`` -- see below) instead of
a local process ID. From then on, SLURM is the source of truth for whether
the job is still pending, running, or finished -- the JobServer polls
``squeue``/``sacct`` rather than watching a local process.

If a job's own flowchart run finishes normally (successfully or not), it
writes its own final status to the datastore directly, exactly as in local
mode -- the JobServer doesn't need to do anything further. The JobServer
only steps in if a tracked job's SLURM state goes missing or terminal while
its datastore row still says ``running`` -- meaning the run never got a
chance to record its own outcome (a node failure, an out-of-memory kill, a
cancellation, SLURM losing the record entirely, or the JobServer itself
having been restarted). In that case it:

1. Trusts the job's own ``job_data.json`` if the run got far enough to write
   one, rather than resubmitting needlessly.
2. Otherwise resubmits, up to ``max_resubmits`` times -- safe because
   flowcharts checkpoint completed steps and resume from the first
   incomplete one, so a resubmitted run picks up where it left off rather
   than starting over.
3. Beyond the cap, gives up and marks the job ``error``.

This same reconciliation runs both during normal polling and when the
JobServer itself restarts (it re-checks every job still marked ``running``
against SLURM on startup) -- restarting the JobServer does not lose track of
or duplicate submissions for jobs that are still genuinely alive in SLURM.

Not yet supported
------------------

- Routing a job to a *specific* section when a config file has more than
  one (all jobs currently use the ``[DEFAULT]`` section).
- Per-step SLURM submission (only whole-flowchart submission exists today).

Index
=====

* :ref:`genindex`
