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

The file has one section per queue (cluster, or a plain local subprocess --
see "Multiple queues" below) this JobServer instance can route jobs to:

.. code-block:: ini

    [DEFAULT]
    # which section a job uses when it doesn't request one explicitly --
    # required if there's more than one section and none of the shortcuts
    # below apply
    default = molssi10

    [molssi10]
    # type: slurm (the default if omitted) | local -- see "Multiple
    # queues" below for type = local
    type = slurm
    # transport: local (SLURM CLI on this host) | ssh (SLURM CLI on a
    # remote host, reached over passwordless SSH)
    transport = local
    # only used when transport = ssh
    # host = molssi10
    # See "No shared filesystem" below for remote_root/
    # remote_run_from_jobserver/remote_conda_env, also ssh-only.

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
    # how many jobs this instance keeps outstanding in this queue at once
    max_concurrent_jobs = 20
    # how many times to resubmit a job SLURM lost track of (see below)
    # before giving up and marking it "error"
    max_resubmits = 3

A blank value (``account =``) means "don't pass that directive at all" --
let SLURM's own defaults apply.

If there's exactly one section (besides ``[DEFAULT]``), it's used
automatically and ``default =`` is optional.

Multiple queues
-----------------

A config file can describe more than one queue, and a job can ask for a
specific one via ``parameters["queue"]`` (a plain string, e.g.
``{"cmdline": [...], "queue": "molssi10"}``) -- a JobServer instance with,
say, ``local`` and ``molssi10`` sections can run some jobs here and dispatch
others to a real cluster, from one process:

.. code-block:: ini

    [DEFAULT]
    default = local

    [local]
    # No scheduler at all -- runs as a plain local subprocess, the same as
    # if no ini file existed. SLURM-only keys (transport, partition,
    # max_resubmits, ...) don't apply to a type = local section.
    type = local
    max_concurrent_jobs = 5

    [molssi10]
    type = slurm
    transport = local
    partition = batch
    max_concurrent_jobs = 20

A job that doesn't set ``parameters["queue"]`` at all uses the instance's
default queue (``[DEFAULT] default =``, or the sole section if there's only
one) -- existing jobs/configs from before this feature existed are
unaffected. A job that requests a queue this instance doesn't have
configured, or requests none at all when there's no default to fall back
to, fails immediately as a ``startup error`` rather than silently running
somewhere unintended.

Each queue's ``max_concurrent_jobs`` is enforced independently -- one queue
being full never blocks another from accepting new jobs. SLURM polling is
batched per queue (one ``squeue``/``sacct`` call per distinct cluster, not
per job), which also matters for correctness: two different clusters can
coincidentally reuse the same SLURM job id number, and per-queue batching
keeps those from ever being compared against each other.

What happens
------------

When a job comes up for submission, the JobServer builds a script wrapping
the same ``run_from_jobserver`` command it would otherwise run directly --
no ``conda activate`` or other shell setup is needed for ``transport =
local``, since the command already uses the full path to the active Python
environment's ``run_from_jobserver``, and SLURM inherits the submitting
environment by default. (``transport = ssh`` is different -- see "No
shared filesystem" below.) The script is submitted via ``sbatch``'s
standard input, not written to a file the target host needs to see. A
copy is also saved to the job's own working directory, as
``slurm_submit.sh``, for reference.

The job's datastore row records the returned SLURM job ID (in its
``parameters`` JSON, alongside a ``resubmit_count`` -- see below) instead of
a local process ID. From then on, SLURM is the source of truth for whether
the job is still pending, running, or finished -- the JobServer polls
``squeue``/``sacct`` rather than watching a local process.

Unlike the running flowchart itself, the JobServer -- not the job -- is
what writes a job's final status to the datastore, in both SLURM and
local-subprocess mode. (The job still writes its own ``job_data.json`` in
its working directory, same as always; the JobServer just reads that back
rather than trusting the job to reach the datastore directly, which isn't
even possible for a job dispatched to a remote host with no shared
filesystem.) The JobServer also adds ``queue`` (and, for a SLURM job, its
``slurm_job_id``) to ``job_data.json`` itself once the job finishes --
the running flowchart process has no notion of "queue" at all, so this is
the one place a user can see which cluster a job actually ran on. Once
SLURM reports a job's state as terminal (or has no record of it at all),
the JobServer:

1. Trusts the job's own ``job_data.json`` if the run got far enough to
   write one, and writes that outcome (``finished``/``error``) to the
   datastore.
2. Otherwise resubmits, up to ``max_resubmits`` times -- safe because
   flowcharts checkpoint completed steps and resume from the first
   incomplete one, so a resubmitted run picks up where it left off rather
   than starting over.
3. Beyond the cap, gives up and marks the job ``error``.

This same reconciliation runs both during normal polling and when the
JobServer itself restarts (it re-checks every job still marked ``running``
against SLURM on startup) -- restarting the JobServer does not lose track of
or duplicate submissions for jobs that are still genuinely alive in SLURM.

This also covers the moment SLURM mode is enabled for the first time on an
instance that already has jobs running as ordinary local subprocesses (or
had one crash before it got a chance to record its own outcome): each
``running``/``kill`` row is reattached based on what was actually recorded
for that specific job -- a local process id or a SLURM job id -- not on
whether SLURM is configured right now. A pre-existing local job is never
mistaken for a lost SLURM submission just because SLURM has since been
turned on.

No shared filesystem
---------------------

If the JobServer doesn't share a filesystem with the SLURM cluster at all
-- for example, it runs on a laptop reaching a remote cluster over ssh --
``transport = ssh`` alone isn't enough: the remote host can't see the
job's working directory, and there's no local Python environment path for
it to invoke either. Two more options in the same ini section (both
``ssh``-only) handle this:

.. code-block:: ini

    [molssi10]
    transport = ssh
    host = molssi10

    # Base directory on the remote host under which each job gets its own
    # scratch directory (named after the job's own directory, e.g.
    # Job_000123) -- created automatically, no need to pre-create it.
    remote_root = /home/psaxe/seamm_remote_jobs

    # How to invoke run_from_jobserver on the remote host. Prefer an
    # explicit absolute path (no shell/conda activation needed, same as
    # local mode):
    remote_run_from_jobserver = /home/psaxe/miniconda3/envs/seamm/bin/run_from_jobserver
    # ...or, if that path isn't known/stable, fall back to a conda
    # environment name (requires `conda` on PATH for the non-interactive
    # ssh session sbatch runs under):
    # remote_conda_env = seamm

Before submission, the job's working directory is pushed to
``<remote_root>/<job directory name>`` on the remote host (over
``rsync -e ssh``); the ``#SBATCH --chdir`` directive and the command line
built for the job both use that remote path, not the local one. Once
SLURM reports the job terminal, the same directory is pulled back before
the JobServer reads ``job_data.json`` -- so results (structures, logs,
``references.db``, ...) end up in the job's ordinary local directory same
as any other job. A transfer failure at either point is treated as
transient and retried the next poll cycle, not recorded as the job
having failed.

Referenced input files need no special handling here -- a flowchart
control parameter of type ``file`` is already staged into the job's own
``data/`` directory (with its path on the command line rewritten to
``job:data/...``) at submission time, before the JobServer ever sees the
job, so the job's working directory is already self-contained by the time
staging runs. The one thing this does *not* handle is a ``job://<n>/...``
cross-job file reference (e.g. reading another job's checkpoint) -- that
points outside the referencing job's own working directory and will fail
to resolve on the remote host.

Per-job resource overrides
---------------------------

Every job from a given JobServer instance otherwise gets the *same* fixed
resource request (cores, memory, walltime, partition, ...) from the ini
file above. A specific job can ask for something different by setting
``parameters["slurm"]`` on its job row, e.g.
``{"cmdline": [...], "slurm": {"ntasks": 4, "mem": "40G"}}``.

Which directives a job is allowed to override, and within what bounds, is
controlled by an **optional** companion section, ``[<section-name>.limits]``
-- secure by default: if it's absent, nothing is overridable, no matter
what a job's ``parameters["slurm"]`` asks for.

.. code-block:: ini

    [molssi10.limits]
    # Only fields listed here can be overridden per-job at all. Anything
    # not listed is fixed by the site config above.
    overridable = partition, ntasks, mem, time

    # Enumerated choice -> the value must be one of these.
    partition.choices = batch, gpu

    # Numeric/size/time bounds -- optional per field. "Overridable with no
    # bound" just means "any value SLURM itself accepts."
    ntasks.min = 1
    ntasks.max = 6
    mem.max = 100G
    time.max = 04:00:00

The JobServer re-validates every override itself before submitting --
against ``overridable``, then ``.choices``/``.min``/``.max`` when present --
regardless of whatever already constrained the request's origin (e.g. a web
UI). A job with an unauthorized or out-of-bounds override is marked
``startup error`` rather than silently run with different resources than
requested, or with the override silently dropped.

If a job needs to be resubmitted (see above), the same override is reused
for every attempt -- it isn't re-validated against a config that might have
changed in the meantime, since the section was already validated against at
submission time.

This ini format is implemented in ``seamm_slurm.config`` (not
``seamm_jobserver`` itself), specifically so other, more lightweight
consumers can read and validate it without depending on the rest of the
SEAMM stack -- ``seamm_webui``'s ``GET /api/queues`` (which the Tk desktop
submit dialog's queue picker and ``.limits``-driven override fields read)
is exactly this kind of consumer.

Not yet supported
------------------

- Per-step SLURM submission (only whole-flowchart submission exists today).
- A retry cap on stage-out transfer failures for a "no shared filesystem"
  section, separate from ``max_resubmits`` -- today a permanently
  unreachable remote host would retry indefinitely rather than eventually
  giving up.

Stopping a job
================

This applies in both local-subprocess and SLURM mode. The JobServer checks
for two things every poll cycle, for every job it is actively tracking:

1. **The job's datastore row was deleted.** Deleting a job (e.g. via the
   dashboard) removes its row and files, but by itself does not touch
   whatever is actually running it. The JobServer notices the row is gone
   and actively stops the run -- ``scancel`` in SLURM mode, terminating the
   process in local mode -- rather than leaving it to run on until it
   crashes on its own missing files (or, on a cluster, sits consuming a
   node for however long that takes).
2. **The job's ``status`` was set to ``kill``.** This stops the run the same
   way, but -- unlike deleting the job -- leaves its row and files alone.
   Any client can request this with an ordinary status update (e.g. the
   dashboard's existing job-update endpoint); no new API is needed. Once
   the JobServer has stopped the job, it sets ``status`` to ``killed``. A
   job that is asked to stop before the JobServer ever started it (still
   ``submitted``) is simply finalized as ``killed`` directly.

Both checks also run as part of startup reattachment, so a kill requested
right before the JobServer restarts is not lost -- it stops the job (or
finalizes it as ``killed``, if it had already ended) instead of resuming
tracking or, worse, resubmitting it as if it had merely gone missing.

Index
=====

* :ref:`genindex`
