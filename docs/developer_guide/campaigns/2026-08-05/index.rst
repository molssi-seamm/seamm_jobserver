2026-08-05 -- SLURM submission for JobServer
=============================================

Status: Phases 0-4, 6, 7, and 8 done (SLURM version/JSON groundwork, the
``seamm_slurm`` backend library, JobServer's SLURM mode, real end-to-end
validation on MolSSI10 -- including a live-discovered per-job resource
override feature, proactive job-stopping, and now real ssh-transport
remote dispatch with no shared filesystem -- and this documentation).
Phase 5 (a future ``seamm_exec`` ``Slurm`` executor for per-step submission)
is not started and not currently scheduled. Phase 8 (remote/no-shared-
filesystem JobServer dispatch -- stage-in/stage-out, plus moving datastore
status writes from the job itself to the JobServer for all job types) is
implemented and validated live against molssi10 (2026-08-08); not yet
committed/released, and not yet enabled for real use (see the Phase 8
section for what's left, which is operational -- a real remote conda env
-- not code). ``~/SEAMM_DEV/Mac.ini`` (dev JobServer only) holds the
validated-working config, deliberately left inert. Originally tracked as
a workspace-root living planning doc;
moved here once the campaign was substantially complete.

All PRs from this campaign are now merged and released:
`seamm_jobserver #17 <https://github.com/molssi-seamm/seamm_jobserver/pull/17>`_
(the SLURM mode, per-job overrides, docs, and this campaign doc) released as
``2026.8.6``; `seamm_exec #30 <https://github.com/molssi-seamm/seamm_exec/pull/30>`_
(the ``job_data.json`` header bugfix) released as ``2026.8.6``.
``seamm_slurm`` had two merged PRs and two releases: `#1
<https://github.com/molssi-seamm/seamm_slurm/pull/1>`_ (docs) released as
``2026.8.6``, and `#3 <https://github.com/molssi-seamm/seamm_slurm/pull/3>`_
(the ``config`` module + ``.limits``/``merge_overrides``) released as
``2026.8.6.1``. ``2026.8.6`` itself never reached PyPI -- its release event
was published during a GitHub Actions outage
(2026-08-06 15:22 UTC - 2026-08-07 02:04 UTC) and appears to have been lost
rather than queued, so nothing retroactively triggered it once GitHub
recovered. Not a functional problem since ``2026.8.6.1`` is a strict
superset and did publish successfully (confirmed on PyPI) once cut after
the outage cleared -- just a gap in PyPI's version history for that one
tag. All three packages' PyPI versions confirmed live: ``seamm_slurm``
``2026.8.6.1``, ``seamm_exec`` ``2026.8.6``, ``seamm_jobserver`` ``2026.8.6``.

Why
---

``JobServer.start_job()`` used to do ``psutil.Popen(run_from_jobserver,
...)`` directly on whatever host the JobServer ran on -- no SLURM
involvement, and **no concurrency limit at all**: every ``submitted`` job in
the datastore started immediately. Fine on a personal Mac, wrong on a shared
cluster.

``seamm_exec`` was already partially SLURM-*aware* (not SLURM-*integrating*):
``computational_environment()`` reads ``SLURM_*`` env vars to size
``NTASKS`` etc., and ``Base.run()`` auto-picks in-place vs scratch-dir
execution based on ``SLURM_JOB_ID``. That machinery exists purely for the
case where a human hand-wraps a flowchart run in ``sbatch``. Nothing
actually called ``sbatch`` itself before this campaign.

Two long-term models (both wanted eventually)
-----------------------------------------------

1. **Option 1 -- whole-flowchart submission.** JobServer submits the entire
   flowchart run (``run_from_jobserver``) as one SLURM job. Simple, and
   necessary on clusters with real queue wait, since it's one queue wait per
   job rather than one per step.
2. **Option 2 -- per-executable submission.** ``seamm_exec`` gains a
   ``Slurm`` executor alongside ``Local``/``Docker``; the flowchart driver
   keeps running locally (as today) and only the heavy codes (ORCA, LAMMPS,
   VASP, MOPAC...) get individually submitted, sized from real per-step data
   (``computational_environment()`` already does this). Better on
   personally-controlled machines / near-zero-queue-wait clusters, since
   lightweight steps (I/O, DB, control flow) never touch the scheduler --
   but needs SEAMM to decide, per step, *whether* to go through SLURM and
   with what shape, which is real new logic, not just plumbing.

Decision: **build Option 1 first.** Design the SLURM-talking pieces
(submit / poll / cancel / status-map) as a shared, reusable component from
the start so Option 2 is an additive consumer of the same backend later, not
a rewrite.

Guiding decisions (locked in)
-------------------------------

- **Option 1 first.** Whole-flowchart ``sbatch`` submission via JobServer.
  Option 2 (``seamm_exec`` ``Slurm`` executor) is explicitly future work --
  not in scope for this campaign, but the shared backend must not preclude
  it.
- **Dual transport, decided at config time, not hardcoded.** JobServer may
  run *on* the SLURM cluster (e.g. a head/login node with ``sbatch``/
  ``squeue``/``sacct`` on ``PATH`` and working munge auth) **or** on a
  separate server that reaches the cluster via passwordless SSH. Both must
  be supported through one interface (``LocalSlurm`` vs ``SshSlurm``
  transports), selected by config, not by which package is installed.
- **SLURM is the source of truth for job state**, not a locally-tracked
  pid. JobServer stores the returned SLURM job ID (in the existing
  ``parameters`` JSON column -- no datastore migration needed, same pattern
  already used for ``pid``/``cmdline``) and polls SLURM (``squeue``/
  ``sacct``) to learn whether a job is pending/running/finished/failed,
  rather than watching a local process.
- **Crash recovery leans on flowchart restartability, not fragile
  reattachment.** SEAMM flowcharts already checkpoint completed steps and
  resume from the first incomplete one. So on JobServer restart, there's no
  need for delicate process-reattachment logic: look up the last known
  SLURM job ID for each ``running`` job, ask SLURM its state; if it's still
  alive, leave it; if it's gone/failed and the flowchart isn't marked done,
  it is **safe to just resubmit** -- the flowchart will skip
  already-completed steps. Needs a retry cap/backoff so a genuinely-broken
  step doesn't resubmit forever.
- **Add a configurable concurrency cap.** JobServer had none. Once compute
  is scheduled via SLURM, the local concern shifts to "how many jobs does
  this JobServer instance keep outstanding in SLURM at once" (submission
  hygiene / not hammering the scheduler on a big batch, per the existing
  TinkerCliffs "job array" concurrent-write lesson) -- configurable, not a
  hardcoded constant.
- **First deployment/validation target: MolSSI10**, not ChemAI. ChemAI is
  live production (its own JobServer has processed thousands of jobs); not
  worth risking disrupting it. MolSSI10 runs an equivalent live setup
  (systemd ``org.molssi.seamm.{dashboard,jobserver}.service``, ``seamm``
  conda env) but was confirmed idle and lower-stakes to iterate against.
  TinkerCliffs stays a later target after MolSSI10 (and ideally ChemAI) are
  proven out.
- **Dev loop:** edit source on a Mac checkout, validate by deploying/
  running on the target cluster over SSH. No shared filesystem exists
  between a laptop and either cluster host (confirmed during Phase 0) --
  deploy via git/rsync + remote install, not by assuming paths line up.
  This is orthogonal to the dual-transport question above, which is about
  how the *deployed* JobServer talks to SLURM, not how its code gets there.

Phase 0 findings
-----------------

Groundwork done directly against ChemAI and MolSSI10 over SSH.

- **SLURM versions differ across targets, and JSON support is not
  uniform.** ChemAI: SLURM 21.08.5, ``squeue --json`` / ``sacct --json``
  both work cleanly (OpenAPI v0.0.37). MolSSI10: SLURM 20.11.4, **``--json``
  is not recognized by either command** (``squeue: unrecognized option
  '--json'``). This made "prefer JSON, text as a fallback" a firm Phase 1
  requirement rather than a nice-to-have: ``SlurmBackend`` needed to support
  both from day one. ``sacct --parsable2 --format=...`` is a clean, stable
  pipe-delimited path for the non-JSON case; ``squeue --format=...``
  similarly. Use JSON when available (self-describing, no format-string
  coupling), parsable2/format text otherwise.
- **``sbatch`` scripts get a non-interactive, non-login shell, so
  ``.bashrc``'s conda-init block never runs.** Confirmed on both ChemAI and
  MolSSI10: both have the standard Ubuntu/Debian ``.bashrc`` guard (``case
  $- in *i*) ;; *) return;; esac`` near the top) that returns immediately
  for non-interactive shells -- which is exactly what a SLURM batch script
  gets, and also what ``ssh host cmd`` / ``bash -lc cmd`` get. It initially
  looked like generated sbatch scripts would need to explicitly ``source
  <conda-base>/etc/profile.d/conda.sh && conda activate seamm`` themselves,
  verified end-to-end on MolSSI10 with a real submitted job.

  **Superseded in Phase 2**: this ``conda activate`` dance turned out to be
  unnecessary when the payload invokes the entry point by its full,
  absolute path (e.g. ``/home/psaxe/miniconda3/envs/seamm/bin/
  run_from_jobserver ...``) rather than a bare command name -- confirmed
  with a second real sbatch test on MolSSI10 (``run_flowchart --help`` and
  ``python -c "import seamm_jobserver, seamm_exec"`` both worked with zero
  activation lines, exit 0). ``_build_cmd()`` already resolved this exact
  absolute path for the pre-existing local-mode code (``Path(sys.executable
  ).parent / "run_from_jobserver"``), so ``_start_job_slurm()`` just reuses
  it -- no conda-activation logic was needed in the sbatch script at all,
  only two ``export`` lines for ``SEAMM_JOB_ID``/``SEAMM_JOBSERVER`` (SLURM
  inherits the submitting environment, PATH included, by default).
- **``db_path``'s direct-sqlite-write-at-completion is host-local, and
  that's fine for the LocalSlurm-on-cluster-host case** (in
  ``seamm_exec/exec_flowchart.py``: after a run, the flowchart process
  itself opens ``db_path`` and does one ``UPDATE jobs ...``). Since this
  campaign targets MolSSI10/ChemAI running their *own* JobServer against
  their *own* local datastore, this keeps working unmodified. It would
  **not** work if a JobServer elsewhere tried to dispatch to one of these
  clusters over SSH while keeping its own separate datastore, since
  there'd be no shared filesystem for the remote process to reach --
  relevant to future ``SshSlurm``/Option 2 work, not a blocker here.
- **Both MolSSI10 and ChemAI already run live production**
  ``org.molssi.seamm.dashboard.service`` / ``...jobserver.service`` systemd
  user services. MolSSI10 additionally runs a second, independent
  dashboard+jobserver pair for another user on the same host. Confirmed
  MolSSI10 had zero jobs in ``running``/``submitted`` state and an empty
  ``squeue`` at the time -- safe to experiment against, but still a shared,
  live-configured host: don't restart its systemd services carelessly, and
  prefer testing against throwaway job dirs/scripts before wiring changes
  into the live services.

Config shape: ``<root>/<jobserver-name>.ini``
------------------------------------------------

This is a **system/machine preference** (how does *this* JobServer/host
talk to its scheduler(s)), not a **user** preference, so it does not belong
in ``~/.seamm.d/seamm.ini`` (the argparse-backed file that holds
``[SEAMM]``/``[JobServer]``/``[Dashboard]`` command-line-option defaults --
that file's own history even documents a past migration *away* from keeping
things in ``~/SEAMM``, for exactly the opposite class of settings). It
belongs alongside ``orca.ini``/``lammps.ini``/``dashboards.ini`` at
``<root>`` (``~/SEAMM`` by default) -- those are read directly via
``configparser.ConfigParser()`` by each consumer's own code, keyed off
``seamm_options["root"]``, the same way e.g. ``orca_step``'s
``_orca_config()`` reads ``orca.ini``.

The file is also not simply named ``slurm.ini``. A JobServer may eventually
**route jobs to more than one cluster/queue** -- different dashboards each
funneling to a different cluster over SSH, or a single JobServer routing
across clusters that don't even share the same queueing system (SLURM on
one, PBS on another, plain local on a third). A single global ``slurm.ini``
wouldn't have room for that. Instead: the file is **named after the
JobServer instance** (its ``--name``, default ``socket.gethostname()``) --
``<root>/<jobserver-name>.ini`` -- with **one section per cluster/queue
target** it knows how to reach, not one section per SLURM version/transport
variant. This also naturally supports multiple JobServer instances sharing
one ``<root>``/datastore (each gets its own config file, no collisions) and
keeps the file format stable even if a future section targets a non-SLURM
scheduler.

.. code-block:: ini

    # <root>/<jobserver-name>.ini -- e.g. ~/SEAMM/molssi10.ini
    # One section per cluster/queue this JobServer instance can route jobs to.
    # System/machine config, not a user preference -- lives at <root>, not
    # ~/.seamm.d/seamm.ini, same reasoning as orca.ini/lammps.ini/dashboards.ini.

    [DEFAULT]
    # which section a job uses when it doesn't request one explicitly
    default = molssi10

    [molssi10]
    # type: slurm (only one implemented for now; pbs/lsf/etc. are future
    # targets this shape leaves room for -- not building a multi-scheduler
    # abstraction yet, just not foreclosing it in the file format)
    type = slurm
    transport = local

    # submission defaults -- applied unless a job's own parameters override them
    partition = batch
    account =
    qos =
    nodes = 1
    ntasks = 1
    time = 01:00:00
    mem =
    gpus =

    # how many jobs this JobServer keeps outstanding in this section's
    # cluster/queue at once
    max_concurrent_jobs = 20

    [chemai]
    type = slurm
    transport = ssh
    host = seamm-chemai
    partition = ChemAI
    nodes = 1
    ntasks = 1
    time = 01:00:00
    max_concurrent_jobs = 10

Blank values mean "don't pass that ``#SBATCH`` directive" (let SLURM's own
partition/QOS defaults apply) -- matters since different clusters have
different partitions/accounting setups and not all have real QOS/account
associations configured.

How a job picks which section to route to, when a config file has more than
one, was left as an open question -- not needed for the initial rollout
(MolSSI10 ships with a single section), but the config *format* already
accommodates multiple sections so answering it later won't require a format
change, only new routing logic in ``check_for_new_jobs()``/``start_job()``.

Architecture
------------

Shared piece (small library, consumed by ``seamm_jobserver`` and, later,
``seamm_exec`` -- mirrors how ``seamm_bsse`` was split out as its own
package rather than folded into an existing one): a new package,
`seamm_slurm <https://github.com/molssi-seamm/seamm_slurm>`_.

- ``SlurmBackend`` (``seamm_slurm/backend.py``): ``submit(script,
  job_name=None) -> job_id`` (feeds the full script text to ``sbatch
  --parsable`` on stdin -- never needs the script to exist as a file on the
  target host, sidestepping the no-shared-filesystem finding from Phase 0),
  ``poll_many(job_ids) -> {job_id: JobStatus}`` (one batched ``squeue``
  call, plus one batched ``sacct`` call only for ids ``squeue`` no longer
  lists -- not one call per job), ``cancel(job_id)``.
- ``LocalSlurm(SlurmBackend)`` (``local.py``) -- shells out to ``sbatch``/
  ``squeue``/``sacct``/``scancel`` directly. ``SshSlurm(SlurmBackend)``
  (``ssh.py``) -- same commands over ``ssh <host> ...``. Both only
  implement ``_run``; everything else is shared in the base class.
- JSON-vs-text handled transparently per-backend-instance (probed once,
  remembered): ``squeue --json``/``sacct --json`` when supported,
  ``--parsable2``/``--format=`` text otherwise. ``sacct``'s and
  ``squeue``'s JSON schemas are *not* analogous (``squeue``'s ``job_state``
  is a flat string; ``sacct``'s ``state``/``exit_code`` are nested objects
  -- confirmed against real completed jobs on ChemAI) -- separate parsers,
  not a shared one.
- ``seamm_slurm/status.py``: ``classify(raw_state) -> category``, one of
  ``pending``/``running``/``completed``/``cancelled``/``failed``/
  ``unknown`` -- a SLURM-domain concept only. Deliberately no SEAMM
  ``jobs.status`` mapping in this library -- that's ``seamm_jobserver``'s
  job, keeping ``seamm_slurm`` reusable (including, later, by
  ``seamm_exec``).
- ``seamm_slurm/script.py``: ``build_script(directives, payload) -> str``
  turns a directives dict (matching ``<root>/<jobserver-name>.ini``
  section keys) into ``#SBATCH`` lines + payload; unknown keys pass
  through as ``--key-with-dashes=value``, so the library doesn't need to
  special-case every SLURM option the ini file might carry.

``seamm_jobserver`` changes:

- ``start_job()``: builds an ``sbatch`` script wrapping the existing
  ``run_from_jobserver`` entry point (a copy is also written into the
  job's working directory, as ``slurm_submit.sh``, for auditability,
  alongside ``job_data.json``/``references.db``), submits it via the
  configured ``SlurmBackend``, stores ``slurm_job_id`` in ``parameters``.
- ``check_for_finished_jobs()``: a single batched ``poll_many()`` call per
  cycle over all tracked ``slurm_job_id``\ s, instead of a psutil per-pid
  loop.
- ``start()``'s startup reattachment scan: ``poll_many()`` over jobs
  marked ``running``, using stored ``slurm_job_id``\ s; missing/terminal +
  flowchart incomplete -> safe resubmit (capped/backed-off), the exact
  same reconciliation path used during steady-state polling.
- A configurable ``max_concurrent_jobs``: ``check_for_new_jobs()`` stops
  pulling ``submitted`` rows once the outstanding-in-SLURM count hits the
  cap.
- ``status()``/GUI: local psutil cpu/mem-per-job stats aren't meaningful
  once compute runs on a remote node -- SLURM-mode job entries report
  ``slurm_job_id``/``resubmit_count`` instead, and both the JSON status
  output and the Tk GUI status view were updated not to crash on the
  differently-shaped entries.

All of this is **entirely additive**: a JobServer instance only enters
SLURM mode if ``<root>/<jobserver-name>.ini`` exists; absent, behavior is
byte-for-byte the same as before this campaign.

Phased plan
-----------

Phase 0 -- Groundwork on MolSSI10 (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Target changed from ChemAI (see the guiding decisions above). SLURM version
+ ``--json`` support checked on both MolSSI10 and ChemAI (differs -- see
Phase 0 findings). Passwordless SSH confirmed. ``run_flowchart``'s env/
``.ini``-sourcing under ``sbatch`` confirmed not automatic at the time
(later superseded, see Phase 0 findings). ``<root>/<jobserver-name>.ini``
config shape drafted and reviewed.

Phase 1 -- Shared SLURM backend library (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New package, `seamm_slurm <https://github.com/molssi-seamm/seamm_slurm>`_,
pushed to GitHub on ``main``; not yet released to PyPI. ``SlurmBackend`` +
``LocalSlurm`` + ``SshSlurm``, JSON-or-text polling (both implemented and
exercised), ``script.build_script()``. 46 unit tests green, ``make format
lint install test`` clean. Real end-to-end smoke test against MolSSI10 via
``SshSlurm`` (not just mocks) -- full submit -> pending -> running ->
completed cycle.

Phase 2 -- JobServer integration (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New ``seamm_jobserver/slurm_config.py`` (``load_slurm_config``/
``SlurmSection``, reads ``<root>/<jobserver-name>.ini``, returns ``None``
when absent -- local-subprocess mode is completely unchanged/untouched in
that case). ``jobserver.py``'s ``start_job``, ``check_for_finished_jobs``,
and the startup reattachment scan are each now dispatchers over a
local-mode path (original code, untouched) and a new SLURM-mode path.
Concurrency cap in ``check_for_new_jobs``. Resubmit-on-loss-of-track with a
retry cap in ``_reconcile_stalled_job``, shared by both the steady-state
poll loop and startup reattachment. ``status()``/``gui_status()`` updated
to not crash on SLURM-mode job entries. 29 tests (real temp-sqlite
datastore + a scriptable ``FakeSlurmBackend``, no mocking of SQL), ``make
format lint install test`` clean.

**A real bug the tests caught and fixed**: the finished-jobs cleanup loop
was unconditionally deleting ``self._jobs[job_id]``/``self._times[job_id]``
even when ``_reconcile_stalled_job`` had just resubmitted and repopulated
them -- every resubmitted job was silently losing tracking immediately
after being resubmitted.

Phase 3 -- Validate on MolSSI10 (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Done via a fully isolated setup (a cloned ``seamm-slurm-test`` conda env,
separate root/datastore/job dirs, a unique JobServer ``--name`` so it never
read the live ``molssi10.ini``) -- the live production JobServers on that
host were never touched or restarted. Real ``sbatch`` -> SLURM ->
completion cycle validated end-to-end (a ``FromSMILES``-only job borrowed
from a real production job dir; MolSSI10 has no ORCA environment, so a
heavier flowchart wasn't an option), plus a genuine kill/restart test using
``#SBATCH --begin=now+25`` to deterministically hold a job ``PENDING``
through the kill/restart window (no timing race). All four target
scenarios confirmed via real logs/datastore state, not just inference:

1. Happy path -- submit -> ``finished``, real output files.
2. Resubmit-and-give-up under genuine repeated SLURM failures -- 4 real
   attempts, correct cap, final ``error``.
3. Restart correctly resuming a still-``PENDING`` job with **no duplicate
   submission** -- the core guarantee this phase set out to prove.
4. The ``job_data.json``-trust path avoiding a needless resubmit.

See "Bugs found and fixed during Phase 3" below for two real, pre-existing
issues surfaced by this real-world testing that mocks alone would not have
caught.

Phase 4 -- Config/docs rollout (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``seamm_jobserver``'s previously-blank User Guide now has real content:
local vs SLURM mode, the full ``<root>/<jobserver-name>.ini`` format
(transports, submission directives, ``max_concurrent_jobs``,
``max_resubmits``), what happens at submission time, and the reconciliation
behavior. README/HISTORY updated to match. ``seamm_slurm``'s README/
HISTORY/campaign doc updated to describe itself as actually wired in and
validated rather than expected/future work, plus a correction (no
conda-activate needed after all) -- ``seamm_slurm``'s ``main`` branch is
branch-protected, so that went through a ``dev`` branch and a pull request
rather than a direct push. ChemAI/TinkerCliffs rollout is still deferred
until MolSSI10 usage is solid, per the original plan. This document is
part of Phase 4: moved from a workspace-root scratch planning doc into
this campaign doc.

Phase 5 -- a future ``seamm_exec`` ``Slurm`` executor (not started)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Option 2 from the top of this document: reusing the Phase 1 backend,
driven by ``computational_environment()``'s per-step sizing, with hybrid
selection of Option 1 vs Option 2 per job/cluster. Not started, not
currently scheduled.

Phase 6 -- per-job SLURM resource overrides (implemented, real-world-driven)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Discovered directly from real usage: after Phase 3/4's live deployment on
MolSSI10, every job from a given JobServer instance got the exact same
fixed SLURM resource request. This surfaced two real problems at once when
Paul submitted several real jobs: jobs defaulted to 1 core each with no way
to ask for more, and -- more surprising -- SLURM was reserving the *entire
node's memory* per job (since the ini's ``mem`` was left blank), so only
one job could ever run concurrently on the 6-core node regardless of free
CPU. Fixing the immediate ``mem`` gap in the live ``molssi10.ini`` (setting
an explicit ``mem = 20G``) was a config change, not a code change, and was
confirmed live: three newly-submitted jobs ran genuinely concurrently once
the fix took effect (the three jobs already in flight before the fix kept
their original whole-node reservation and continued running one at a time,
as expected, since a submitted job's resource request is fixed at
submission time).

The design, from there: sites need a way to let *specific* jobs ask for
different resources than the instance-wide default, while still corralling
requests to valid values (Paul: "the site-specific information in
JobServer.ini should also specify limits ... or enumerated choices ... to
corral users to valid specifications").

- A job's requested overrides live in its own ``parameters["slurm"]``
  (e.g. ``{"ntasks": 4, "mem": "40G"}``) -- same place ``cmdline`` already
  lives, no datastore migration.
- Which directives a job may override, and within what bounds, is
  controlled by an optional ``[<section>.limits]`` ini section (enumerated
  ``.choices``, or ``.min``/``.max`` bounds -- unit-aware for ``mem``
  (K/M/G/T) and ``time`` (``HH:MM:SS``/``D-HH:MM:SS``), plain numeric
  otherwise). Secure by default: no ``.limits`` section means nothing is
  overridable.
- ``SlurmSection.merge_overrides()`` validates and merges a request server
  side, always -- never trusts that a caller (e.g. a future web UI)
  already enforced this. Raises ``ValueError`` on anything unauthorized or
  out of bounds, which JobServer's existing ``start_job`` error handling
  already catches and turns into a ``startup error`` status -- no new
  error-handling plumbing needed.
- **Moved the whole ini-parsing/validation module from
  ``seamm_jobserver.slurm_config`` into ``seamm_slurm.config``** (Paul's
  call): both ``seamm_jobserver`` and any future lightweight consumer (a
  job-submission UI) need to read this file, and ``seamm_slurm`` was
  already built to be dependency-light and reusable, unlike
  ``seamm_jobserver`` itself (psutil, GUI code, job-running machinery).
  ``seamm_jobserver`` now just imports ``seamm_slurm.config``.
- **``seamm_dashboard`` will not be getting this feature.** Paul: it's too
  fragile to keep extending. The requirement was instead written into
  ``seamm_webui``'s own living plan doc
  (``~/Work/SEAMM/dashboard-rewrite-plan.md``, that project's session is
  informally called "datastore") for whenever job submission gets built
  out there -- not reachable via direct agent messaging (not a spawned
  teammate in this session), so left as a note in the doc that project
  actually reads.

34 new tests in ``seamm_slurm`` (``FieldLimits``/``.limits`` parsing/
``merge_overrides``, including real ``mem``/``time`` unit-conversion cases)
plus new ``seamm_jobserver`` tests covering the full wiring (override
applied/rejected/out-of-range, preserved across resubmission and restart
reattachment). All passing, lint and docs clean in both packages.

Phase 7 -- proactively stopping deleted or explicitly-killed jobs (implemented, real-world-driven)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Discovered directly from real usage, again: before this campaign, deleting a
job (e.g. via the dashboard) removed its row and files but never told
whatever was actually running it to stop -- it just ran on until it crashed
on its own missing files. Fine (if wasteful) for a local subprocess; on
SLURM, an orphaned job can sit running, or queued, consuming a node/slot for
however long it takes to fail on its own. Paul: "we will need to be more
proactive and kill the slurm job."

The design:

- A new ``check_for_stopped_jobs()`` runs every poll cycle, before
  ``check_for_finished_jobs()`` -- ordering matters, since a job that was
  just killed must already be gone from tracking before the finished-jobs
  reconciliation logic gets a chance to treat it as merely *lost* and try
  to resubmit it.
- **Deleted row**: for every job the JobServer is actively tracking, a
  single batched query checks whether its row still exists at all. If not,
  the JobServer actively stops it (``scancel`` in SLURM mode, ``terminate``/
  ``kill`` in local mode) and drops it from tracking. Nothing to write to
  the datastore -- the row is gone.
- **Explicit kill, files kept**: extended the ask to also support stopping
  a job without deleting it -- setting ``status = 'kill'`` on the row (an
  ordinary status update; no new API needed on the dashboard/webui side,
  since a generic job-update endpoint already exists). The JobServer stops
  the run the same way, then finalizes ``status`` to ``killed``. A job
  killed before the JobServer ever started it (still ``submitted``) is
  simply finalized as ``killed`` directly, no process to touch.
- **Startup reattachment also has to know about this.** Both
  ``_reattach_local_jobs`` and ``_reattach_slurm_jobs`` previously only
  looked at ``status = 'running'`` rows. Extended both to
  ``status IN ('running', 'kill')`` -- a job whose kill was requested right
  before a JobServer restart still needs its *real* process/SLURM job
  cancelled, not silently resumed, and (the sharper bug this would
  otherwise cause) not handed to the ordinary lost-job reconciliation path,
  which would try to resubmit it. If such a job is still alive, it's pulled
  back into tracking (so the next ordinary poll cycle kills it via the path
  above); if it's already gone, it's finalized as ``killed`` directly.
- A new ``killed_jobs`` counter, alongside the existing
  ``successful_jobs``/``failed_jobs``/``ended_jobs``, surfaced in
  ``status()`` and the Tk status tab.

11 new tests (SLURM and local mode, both the deleted-row and kill-status
paths, plus the three reattachment edge cases above) -- 39 total, all
green, lint and docs clean.

Phase 8 -- remote (no-shared-filesystem) JobServer dispatch (designed, not started)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prompted by Paul wanting a JobServer running on his Mac to submit to
MolSSI10's SLURM cluster over SSH -- distinct from every case validated so
far, which is a JobServer that already runs *on* a host sharing storage
with the cluster (MolSSI10's own resident JobServer, reached locally; or
the ``SshSlurm`` transport validated in Phase 1, used there only to drive
``sbatch``/``squeue``/``sacct`` remotely for testing, not to run a whole
disconnected flowchart pipeline). A placeholder
``~/SEAMM_DEV/Mac.ini`` (``transport = ssh``, ``host = molssi10``, dev
JobServer only -- deliberately not added to the live ``~/SEAMM/Mac.ini``,
since a JobServer flips into SLURM mode the moment that file exists, with
no disabled state) was added on 2026-08-08 as a marker for this phase; it
does not work yet. Two real gaps, both grounded in the actual code rather
than assumed:

1. ``seamm_jobserver.jobserver._build_cmd()`` derives the job's executable
   from ``sys.executable`` -- the *submitting* Mac's own conda env path,
   meaningless on the remote host.
2. ``JobServer._start_job_slurm()`` sets ``chdir`` to the job's local
   ``wdir`` (a Mac path) directly as an ``#SBATCH --chdir`` directive. With
   no shared filesystem between the Mac and any cluster host (confirmed
   Phase 0), that directory does not exist remotely, so ``sbatch`` would
   fail immediately.

What's *not* a gap, corrected from an initial over-worry: whether a job's
input files (referenced by absolute path outside ``wdir``) need their own
bespoke transfer logic. They don't -- SEAMM already solves this for the
client-to-Dashboard hop, and the same convention generalizes cleanly to
Dashboard/JobServer-to-remote-cluster. Concretely: a flowchart control
parameter of ``type: "file"`` gets resolved at submission time
(``seamm_dashboard_client.dashboard.Dashboard.submit()``,
``seamm_dashboard/util.py``'s ``safe_filename()``) into a ``job:data/...``
reference on the command line, and the actual bytes are uploaded
separately (``job.put_file()``) into that job's own ``data/`` directory
before the job ever starts. By the time ``check_for_new_jobs()`` picks a
submitted job up, its working directory is already self-contained --
``flowchart.flow``, the ``job_data.json`` stub, and ``data/`` holding every
file the flowchart needs, with ``job:data/...`` references resolved
locally by ``seamm.Node.file_path()``'s ``job:`` URI scheme. So stage-in
for a remote SLURM section is just "rsync the job's own ``wdir``," not a
general absolute-path resolver. **Known
out-of-scope corner case for v1**: ``job://<n>/...`` cross-job references
(another job's checkpoint/output, e.g. ORCA's ``specified orbitals``
restart) point outside the referencing job's own ``wdir`` entirely and
would need the *referenced* job staged too -- not addressed by this
design; such a reference in a job routed to a remote SLURM section should
fail clearly rather than silently resolve to nothing.

The design:

- **A stager, paired with the transport.** Mirrors the existing
  ``LocalSlurm``/``SshSlurm`` pairing in ``seamm_slurm`` (``local.py``/
  ``ssh.py``): a ``LocalStager`` (no-op -- today's on-cluster-JobServer
  case, unchanged) and an ``RsyncStager``, selected by the same
  ``transport`` ini key. ``RsyncStager.stage_in(local_wdir, remote_wdir)``
  runs ``ssh host mkdir -p <remote_wdir>`` then
  ``rsync -e ssh -a <local_wdir>/ host:<remote_wdir>/`` before submission;
  ``stage_out(remote_wdir, local_wdir)`` runs the same rsync in reverse
  once SLURM reports the job terminal, before anything reads
  ``job_data.json`` back on the JobServer side. Lives in ``seamm_slurm``
  (not ``seamm_jobserver``), matching why ``config.py`` moved there in
  Phase 6 -- a lightweight consumer can use the transport without pulling
  in the rest of ``seamm_jobserver``.
- **New per-section ini keys**: ``remote_root`` (base directory under
  which each job's remote scratch tree is created, e.g. a path on
  MolSSI10's own filesystem reachable from its ``sbatch`` host) and either
  ``remote_conda_env`` (payload becomes
  ``conda activate <env> && run_from_jobserver ...`` for ssh-transport
  jobs specifically) or an explicit ``remote_python``. Needed because
  ``_build_cmd()``'s "no conda activation needed, invoke by full absolute
  path" shortcut (the Phase 2 correction to the original Phase 0 finding)
  is itself a Mac-local-path assumption that doesn't survive a remote
  host with a different filesystem layout.
- **Stage-out failure is not job failure.** A network blip during the
  post-completion ``rsync`` pull must not be recorded as the job having
  failed -- it needs its own retry, analogous to but distinct from the
  existing resubmit-count logic in ``_reconcile_stalled_job``, so a
  successful remote run isn't discarded over a transient transfer error.
- **Remote scratch retention.** Nothing today cleans up a remote job's
  staged directory after a successful stage-out; needs an explicit
  policy (delete immediately, or keep N days for debugging) rather than
  letting it accumulate indefinitely on a shared login node.

**Second, orthogonal change, generalized from this phase's needs to all
job types (Paul's call):** move ownership of the datastore's terminal
``jobs.status`` write from the running job itself to the JobServer,
uniformly -- not only for the new remote-SLURM case, where the running
process genuinely cannot reach the JobServer's sqlite file, but for local
and on-cluster-SLURM jobs too.

Today this is inconsistent and, on inspection, already relies on a race
that Phase 3 had to fix once (the ``job_data.json`` header-newline bug) and
documents as "nothing to duplicate ... in the common case" rather than as
a designed guarantee:

- ``exec_flowchart.py``'s ``run()`` (the ``in_jobserver`` branch, after
  unconditionally writing ``job_data.json`` in the same ``finally`` block)
  does a direct ``sqlite3.connect(db_path)`` ``UPDATE jobs...`` against
  the datastore -- for *every* job type, local or SLURM, remote or
  on-cluster. Its failure is silently swallowed
  (``except Exception as e: printer.job(e)``).
- ``JobServer._check_for_finished_jobs_local()`` never touches the
  datastore row at all -- it only updates in-memory counters and stops
  tracking, trusting the child process's own write above entirely.
- ``JobServer._check_for_finished_jobs_slurm()``'s docstring says as much
  outright: "the job's own ``run_from_jobserver`` process still writes the
  datastore's final status ... nothing to duplicate here in the common
  case." Only ``_reconcile_stalled_job()`` -- the *un*-common case --
  actually has the JobServer write the status itself, via
  ``_read_job_data_state()`` (parses ``job_data.json``) +
  ``_finalize_job_status()`` (a conditional ``UPDATE ... WHERE status =
  'running'``, so it never clobbers a write that already landed).

Paul's point: a job process writing to a database it may not even be able
to reach (true today for remote SLURM; also just generally odd for a
worker process to own writes to its owner's datastore) is backwards.
Cleaner ownership: **the JobServer always performs the terminal status
write, for every job type**, using exactly the ``_read_job_data_state()`` /
``_finalize_job_status()`` pair Phase 2/3 already built for the SLURM
reconciliation-only case -- generalized to be the *only* path, not a
fallback:

- ``_check_for_finished_jobs_local()`` changes to read
  ``job_data.json``'s ``state`` once the process exits (mirroring what
  ``_reconcile_stalled_job`` already does) and call
  ``_finalize_job_status()``, instead of relying on the child's own write.
- ``_check_for_finished_jobs_slurm()``'s happy path changes the same way
  -- always finalize from ``job_data.json`` once ``poll_many()`` reports a
  terminal SLURM state (after ``stage_out`` for the ssh-transport case
  above), rather than assuming the remote process already wrote it.
- ``exec_flowchart.py``'s ``in_jobserver`` direct-sqlite-write branch can
  then be deleted outright -- ``job_data.json`` is already written
  unconditionally first, so the JobServer never needs the job to touch
  the database at all. The other branch (``elif not standalone:``, used
  when a flowchart is run by hand against a Dashboard-connected datastore
  with *no* JobServer involved) is unaffected -- that is the one case
  where the running process legitimately is the only thing that can
  record its own outcome.
- Net effect: local- and SLURM-mode finished-job detection converge on
  one shape (detect terminal condition -> read ``job_data.json`` -> one
  ``_finalize_job_status`` call), removing a dual-writer race for every
  job type rather than papering over it only for the new remote case.

Suggested build order: (1) the local/SLURM DB-ownership change, since it's
a strict simplification independent of remote dispatch and de-risks the
rest; (2) the stager abstraction + new ini keys; (3) wire
``stage_in``/``stage_out`` into ``start_job``/``check_for_finished_jobs``
for ``transport = ssh``; (4) unit tests against a fake stager, then one
real end-to-end validation cycle against MolSSI10 in an isolated
root/env/JobServer name, matching Phase 3's discipline of not trusting
mocks alone for this kind of cross-host, cross-format integration.

**Sub-step (1) is done** (2026-08-08): ``_check_for_finished_jobs_local()``
now reads ``job_data.json`` (falling back to the process exit code only if
that file is missing) and calls ``_finalize_job_status()`` itself, the
same as the SLURM path; ``_start_job_local()`` now records ``wdir`` in
``self._jobs[job_id]`` so that read is possible. ``_reconcile_stalled_job``
renamed to ``_finalize_or_resubmit_slurm_job`` and is now the primary
finalize path for every terminal SLURM job (not just a lost-tracking
fallback) -- the premature success/failure counter bump keyed off SLURM's
own ``completed`` category (which only reflects the process's exit code,
not the flowchart's own verdict) was removed in favor of counting once the
real state is known, from ``job_data.json``. ``seamm_exec``'s
``exec_flowchart.run()`` no longer does the direct
``sqlite3.connect(db_path)`` write under ``in_jobserver`` at all --
``db_path`` stays an accepted parameter (and stays on the
``run_from_jobserver`` command line, built by ``_build_cmd()``) purely for
CLI-contract compatibility between independently-released
``seamm_jobserver``/``seamm_exec`` versions, but is otherwise unused now.
5 new/rewritten tests in ``seamm_jobserver`` (local-mode finalization had
*zero* prior coverage -- this was, on inspection, a real pre-existing gap:
local jobs relied entirely on the child process's own write, with nothing
recovering a job stuck at ``running`` if that write never happened).
44/44 and 9/9 tests green, ``make lint`` clean in both packages. Installed
into the ``seamm-dev`` conda env only (not the live ``seamm`` env, which
has two real jobs running) -- not yet committed, and the live JobServer
processes on this Mac were not restarted, so this has no effect on
anything running until that happens deliberately.

**Sub-step (2) is done** (2026-08-08): new ``seamm_slurm.stage`` module --
``JobStager`` ABC, ``LocalStager`` (no-op), ``RsyncStager`` (``ssh ...
mkdir -p`` then ``rsync -e ssh -a`` for ``stage_in``; the reverse rsync for
``stage_out``), raising a new ``StageError`` on either failing. Exported
from the package top level alongside ``LocalSlurm``/``SshSlurm``.
``SlurmSection`` gained a ``build_stager()`` paired with ``build_backend()``
(same transport-keyed dispatch), plus two new optional ini keys parsed by
``load_slurm_config()``: ``remote_root`` (base directory for a job's
remote scratch tree) and ``remote_conda_env`` (needed because
``_build_cmd()``'s Mac-local executable path is meaningless on a remote
host -- not yet consumed by anything, that's sub-step 3). 15 new tests
(94 total in ``seamm_slurm``), ``make lint`` clean, installed editable
into ``seamm-dev``.

**Sub-step (3) is done** (2026-08-08): ``seamm_jobserver`` now actually
calls the stager. Also added a third ini key while wiring this up --
``remote_run_from_jobserver`` (an explicit absolute path, preferred over
``remote_conda_env``'s ``conda run -n <env>`` fallback, since it needs no
shell/conda activation on the remote end, mirroring how local mode already
invokes ``run_from_jobserver`` by absolute path). Concretely:

- ``JobServer._stager`` is built alongside ``_slurm_backend`` (from
  ``self._slurm.build_stager()``) wherever the SLURM config loads.
- ``start_job()`` no longer builds the job's command line before
  dispatching to SLURM mode -- ``_start_job_slurm`` now takes the raw
  ``cmdline`` and builds ``cmd`` itself, *after* staging, since a
  ``transport = ssh`` job's ``#SBATCH --chdir`` and its command line's
  working-directory argument must both be the *remote* path, not the
  local one. New ``_remote_wdir(wdir)`` derives that path deterministically
  from ``remote_root`` + the job directory's name (``Job_NNNNNN`` names are
  unique across the whole datastore, not just per-project, so no collision
  risk); new ``_remote_exe_prefix()`` picks
  ``remote_run_from_jobserver``/``remote_conda_env`` the way described
  above, raising clearly if a ``transport = ssh`` section configures
  neither. The local-mode path through ``_build_cmd`` is unchanged.
- Every place that used to store a job's fully-built ``cmd`` for possible
  resubmission (``self._jobs[job_id]["cmd"]``) now stores the raw
  ``cmdline`` instead (``"cmdline"``), plus a new ``"remote_wdir"`` (``None``
  for ``transport = local``) -- covers fresh submission, resubmission, *and*
  startup reattachment (``_reattach_slurm_jobs`` recomputes ``remote_wdir``
  fresh from ``wdir``, a pure function of config, rather than needing to
  have persisted it).
- ``_finalize_or_resubmit_slurm_job`` calls ``self._stager.stage_out()``
  first, before ever reading ``job_data.json`` -- for
  ``transport = local`` this is a no-op (``data["remote_wdir"]`` is
  ``None``), so nothing changes for every case validated so far. A
  ``StageError`` here is treated as transient and *not* conflated with the
  resubmit-count logic (a different failure mode -- SLURM losing track of
  the job entirely): the method returns ``True`` (stay tracked, retry next
  poll cycle) without touching the datastore row or resubmitting.
- **Known gap, not addressed**: a permanently-broken stage-out (e.g. the
  remote host becomes unreachable for good) would retry indefinitely,
  since nothing currently caps stage-out retries the way
  ``max_resubmits`` caps SLURM-losing-the-job retries. Low priority before
  this phase is even deployed once, but worth a retry cap of its own
  before real use.

9 new tests in ``seamm_jobserver`` (52 total) -- staging call order and
arguments, remote vs. local command-line content, both
``remote_run_from_jobserver`` and the ``conda run`` fallback, the
missing-both-config error, stage-out-failure-retries-then-succeeds across
two poll cycles, and reattachment recomputing ``remote_wdir``. All via
``FakeStager`` (new, alongside ``FakeSlurmBackend``/``FakeProcess``) --
never touches real ssh/rsync. ``make lint`` clean in all three packages,
installed into ``seamm-dev``. Still not committed, and the live JobServer
processes on this Mac were not restarted.

**Sub-step (4) is done -- Phase 8 fully validated live** (2026-08-08):
real, unmocked end-to-end run from this Mac to MolSSI10, no shared
filesystem involved at any point. Isolated test setup, matching Phase 3's
discipline: a throwaway root (``~/seamm_phase8_test``, since removed) with
its own minimal ``jobs`` table (the same shape the unit tests use --
confirmed sufficient, since ``run_from_jobserver`` on the remote side
never touches this database at all post sub-step-1, only
``job_data.json``), one hand-built job row/directory (a real two-node
flowchart -- ``StartNode`` -> ``FromSMILESStep``, built via ``seamm``'s
own API rather than hand-authoring flowchart JSON), and a
``phase8-test.ini`` pointing ``transport = ssh`` at ``molssi10`` with
``remote_run_from_jobserver`` set to the ``seamm-slurm-test`` conda env
left over from Phase 3. Ran the real ``seamm-jobserver`` console script
(not a Python test harness) against this setup.

Confirmed, independently, at every stage:

- ``RsyncStager.stage_in`` really pushed the job directory to
  ``molssi10:/home/psaxe/seamm_phase8_test/remote/Job_000001`` over real
  ``ssh``/``rsync``.
- The generated ``slurm_submit.sh`` (local debug copy) had
  ``--chdir=/home/psaxe/.../remote/Job_000001`` (the *remote* path) and
  invoked ``remote_run_from_jobserver`` directly -- the local Mac path
  never appeared in the script at all.
- ``sacct`` on molssi10 independently confirmed job 24 ``COMPLETED``,
  exit code ``0:0``.
- The flowchart genuinely ran remotely: ``job.out`` (pulled back via
  stage_out) shows "Created a molecular structure with 3 atoms" (water,
  from ``SMILES=O``) and ``~cpuinfo`` in the pulled-back ``job_data.json``
  identifies molssi10's real Xeon E5-1650, not the Mac's Apple Silicon.
- ``RsyncStager.stage_out`` pulled everything back --
  ``final_structure.mmcif``, ``references.db``, the per-job structure
  ``seamm.db``, ``slurm-24.out``, and ``job_data.json`` (``"state":
  "finished"``) all landed in the *local* job directory.
- The JobServer (not the remote job) finalized the datastore: local
  ``jobs.status`` read back ``finished`` after the run, confirming
  sub-step (1)'s design -- the remote ``run_from_jobserver`` process
  never touched the local sqlite file at all, by design.

One unrelated, pre-existing cosmetic artifact noted, not a Phase 8 bug: a
stray "unable to open database file" line in ``job.out``, right before
the final timestamp print. Confirmed present in real pre-existing local
production job logs too (``~/SEAMM_DEV/Jobs/.../thermal conductivity/
Job_000808/job.out`` and others, predating this campaign entirely) -- some
unrelated resource's ``__del__``/atexit cleanup, not on the success path
(``job_data.json``'s ``state`` is already written and read back correctly
before this point). Not investigated further as part of Phase 8.

Both the local (``~/seamm_phase8_test``) and remote
(``/home/psaxe/seamm_phase8_test``) test trees were removed after the
run. ``~/SEAMM_DEV/Mac.ini`` (the placeholder from earlier sub-steps)
updated to record that the mechanism is now validated-working, while
staying deliberately inert for real dev use -- ``remote_run_from_jobserver``
still points at the shared, stale ``seamm-slurm-test`` env from Phase 3,
not a real/current/dedicated environment, so restarting the dev JobServer
today would route real work through an environment not meant for it.
That's the one remaining step before this could become MolSSI10's actual
day-to-day dev-dispatch target.

**Phase 8 implementation is now complete** (sub-steps 1-4 all done); what
remains before real use is operational, not code: a dedicated (non-test)
remote conda env with a matching install, and a considered decision about
which JobServer instance(s) should actually run with this enabled.

Bugs found and fixed during Phase 3
--------------------------------------

Real end-to-end testing surfaced two genuine, pre-existing issues that the
mocked Phase 1/2 unit tests could not have caught (their fakes were
written to be internally consistent, so they couldn't reveal a real
mismatch between two real writers/readers of the same file format):

1. **``seamm_exec/exec_flowchart.py``'s ``run_from_jobserver()`` exception
   handler wrote ``job_data.json``'s header without a trailing newline**
   (``fd.write("!MolSSI job_data 1.0")``, unlike every other writer of
   this file, which uses the module-level ``header_line = "!MolSSI
   job_data 1.0\n"`` constant). Consequence: the header text and the JSON
   blob landed on the same first line, so a ``readline()``-then-
   ``json.load()`` reader (what ``_read_job_data_state()`` originally did,
   and what ``seamm_datastore.Job.parse_job_data`` also does) failed to
   parse the file and silently treated it as absent. Effect on this
   project specifically: ``_reconcile_stalled_job`` couldn't tell a job
   had already recorded its own outcome, and resubmitted it needlessly.
   Fixed at the source (use ``header_line``, matching every other writer)
   plus made ``_read_job_data_state()`` itself tolerant of both forms
   (strip up to the first ``{`` rather than assuming a clean header line)
   as defense in depth, since old already-written files may still have
   the bug. This also fixes a live (if rare) correctness gap in
   ``seamm_datastore.Job.parse_job_data`` for any job that fails via this
   exact exception path, independent of SLURM entirely. The
   ``seamm_exec`` side of this fix is
   `PR #30 <https://github.com/molssi-seamm/seamm_exec/pull/30>`_ (open,
   not yet merged).
2. **Real submissions always pre-create a ``job_data.json`` stub before
   the flowchart ever runs** (confirmed in
   ``seamm_dashboard/routes/api/jobs.py`` at job-creation time) --
   ``run()`` unconditionally expects to read it back when
   ``in_jobserver=True``. This is not a bug to fix, just a real
   precondition this campaign's hand-built test job rows initially missed
   (only ``flowchart.flow`` was copied over, not the stub), which is what
   surfaced bug #1 in the first place. Noting it here because it's a real
   invariant: anything that creates a job row for JobServer to pick up (a
   future ``seamm_exec`` Phase 5, or any other tooling) must also write
   this stub, not just the flowchart file.

Status log
----------

- **2026-08-05** -- Requirements/architecture discussion with Paul. Locked
  in: Option 1 first (Option 2 deferred but backend shared), dual
  transport (local CLI / SSH), SLURM as source of truth for state,
  resubmit-on-crash leaning on existing flowchart restartability, add a
  concurrency cap (previously absent), first target = ChemAI (not
  TinkerCliffs). Plan written; implementation not started.
- **2026-08-06** -- Phase 0 groundwork done, from a Mac checkout via SSH
  to both clusters. Target changed: ChemAI turned out to be live
  production (its JobServer had processed thousands of jobs) -- switched
  the first validation target to MolSSI10, which runs an equivalent live
  setup but was idle at the time. Key findings: SLURM versions/JSON
  support differ across targets (backend needs both paths from day one);
  ``sbatch`` scripts appeared to need explicit ``conda activate``
  (verified working end-to-end with a real job, later superseded in
  Phase 2); no shared filesystem exists between a laptop and either
  cluster (relevant to future ``SshSlurm``/Option 2 work, not a blocker
  for Phases 1-3, which target each cluster's own already-resident
  JobServer).
- **2026-08-06 (later)** -- Paul corrected the config location: SLURM
  settings are a system/machine preference, not a user preference, so
  they belong at ``<root>``, alongside ``orca.ini``/``lammps.ini``/
  ``dashboards.ini``, **not** as a section in ``~/.seamm.d/seamm.ini``.
  Verified against ``seamm_util/argument_parser.py`` and
  ``orca_step``'s ``_orca_config()``. Plan's config-shape section
  corrected. Phase 0 fully done.
- **2026-08-06 (further thought)** -- Paul: don't name the file
  ``slurm.ini`` either -- a JobServer may eventually route jobs to
  multiple clusters/queues, possibly with different queueing systems.
  Renamed the design to ``<root>/<jobserver-name>.ini``, with one section
  per cluster/queue target instead of one section per transport variant.
- **2026-08-06 (Phase 1)** -- Built ``seamm_slurm`` (skeleton copied from
  ``seamm_bsse``'s boilerplate and adapted): ``SlurmBackend``/
  ``LocalSlurm``/``SshSlurm``/``status.py``/``script.py``. Real ``sacct
  --json`` schema pulled from a live completed job on ChemAI before
  writing the parser (nested ``state.current``/``exit_code.return_code``,
  unlike ``squeue``'s flat ``job_state`` string -- would have gotten this
  wrong by guessing). 46 unit tests, lint clean. Validated for real (not
  just mocked) against MolSSI10 via ``SshSlurm``.
- **2026-08-06 (Phase 1, committed)** -- Paul created the empty
  ``molssi-seamm/seamm_slurm`` GitHub repo; committed the scaffold and
  pushed to ``main``. Phase 1 fully done.
- **2026-08-06 (Phase 2)** -- Wired ``seamm_slurm`` into
  ``seamm_jobserver``. Discovered (and reused) that full-path invocation
  of ``run_from_jobserver`` needs no ``conda activate`` under ``sbatch``
  -- simpler than the Phase 0 finding assumed. New
  ``<root>/<jobserver-name>.ini``-driven SLURM mode is fully additive: no
  ini file present means zero behavior change (verified by the
  local-mode tests continuing to pass unmodified). 29 tests, lint clean;
  a real cross-tracking bug (resubmitted jobs losing tracking) was found
  and fixed by the tests, not by inspection. Deliberately did not touch
  MolSSI10's live JobServer as part of this pass.
- **2026-08-06 (Phase 3)** -- Validated for real on MolSSI10, via a fully
  isolated setup (cloned conda env, separate root/datastore/job dirs,
  unique JobServer name) -- confirmed the live production JobServers on
  that host kept running throughout, untouched. Real bugs found and
  fixed along the way (see "Bugs found and fixed during Phase 3" above).
  After the fix: clean happy-path run; genuine resubmit-and-give-up
  validated under real repeated SLURM failures; and a deterministic
  kill/restart-while-``PENDING`` test confirmed reattachment resumes
  tracking an existing SLURM job with no duplicate submission -- the
  core guarantee this phase set out to prove.
- **2026-08-06 (Phase 4)** -- Docs rollout: ``seamm_jobserver``'s User
  Guide now documents the SLURM mode for real; README/HISTORY updated.
  ``seamm_slurm``'s README/HISTORY/campaign doc updated similarly. Paul
  made ``seamm_slurm``'s ``main`` branch-protected -- used a ``dev``
  branch and a pull request
  (`PR #1 <https://github.com/molssi-seamm/seamm_slurm/pull/1>`_, left
  open for review, not merged) rather than a direct push.
- **2026-08-06 (this document)** -- Moved from a workspace-root scratch
  planning doc (``jobserver-slurm-plan.md``, not version-controlled) into
  this campaign doc, per Paul's request, since the campaign was
  substantially complete and the plan spans this package plus
  ``seamm_slurm``/``seamm_exec``. The original scratch file now just
  points here.
- **2026-08-06/07 (live deployment + Phase 6)** -- Deployed to the live
  MolSSI10 JobServer (patched conda env, restarted the systemd service).
  Paul submitted real jobs and found only one ran at a time; root-caused
  to SLURM defaulting to whole-node memory reservation (``mem`` blank in
  ``molssi10.ini``) rather than a core-count issue -- fixed live with an
  explicit ``mem = 20G``, confirmed concurrent jobs after. This directly
  motivated Phase 6 (per-job resource overrides with site-defined limits),
  designed and implemented the same session, then validated live against
  MolSSI10 with a real accepted override (``ntasks=3``, confirmed via
  ``sacct``) and a real rejected out-of-range override (no SLURM job
  created, correct error surfaced as ``startup error``). Redeployed the
  final code plus a real ``[molssi10.limits]`` section
  (``overridable = ntasks, mem, time``) to MolSSI10. Test job dirs
  (``Job_000660``-``Job_000668``) cleaned up afterward, leaving the ~130
  pre-existing real jobs in the same project untouched.
- **2026-08-07 (releases)** -- All three packages' PRs merged by ``seamm``
  and released: ``seamm_slurm`` ``2026.8.6.1`` (the ``2026.8.6`` tag was
  lost to a GitHub Actions outage and never reached PyPI, see the Status
  paragraph above), ``seamm_exec`` ``2026.8.6``, ``seamm_jobserver``
  ``2026.8.6``. All confirmed live on PyPI; local checkouts synced via
  ``make update``.
- **2026-08-07 (Phase 7)** -- Paul: deleting a job in the dashboard leaves
  whatever is actually running it untouched, which SLURM makes worse than
  it was locally (an orphaned job can occupy a node/slot for a long time
  before crashing on its own). Designed and implemented proactive
  stopping: a deleted row or an explicit ``status = 'kill'`` (new,
  files-preserving stop mechanism) is now noticed every poll cycle and
  actively cancelled/terminated, with startup reattachment also covering
  ``kill``-status rows so a kill requested right before a restart isn't
  lost or wrongly resubmitted. 11 new tests (39 total), lint/docs clean.
- **2026-08-08 (Phase 8, designed)** -- Paul wants a JobServer on his Mac
  able to submit to MolSSI10's SLURM cluster over SSH -- a case not
  actually covered by anything validated so far, since the Mac shares no
  filesystem with any cluster host. Added a non-functional placeholder
  ``~/SEAMM_DEV/Mac.ini`` (dev JobServer only) as a marker. Re-reading
  ``_build_cmd()``/``_start_job_slurm()``/``exec_flowchart.run()``
  confirmed two real gaps (Mac-local executable path, Mac-local
  ``--chdir``) plus a third already documented as a known limitation
  (``exec_flowchart.py``'s ``in_jobserver`` datastore write is
  unreachable from a remote host). Paul corrected two parts of the initial
  design sketch: (1) referenced input files need no bespoke transfer
  logic -- the existing flowchart ``type: "file"`` control-parameter
  mechanism already copies them into the job's own ``data/`` before the
  JobServer ever sees the job, so remote stage-in is just "rsync the
  job's ``wdir``," full stop; (2) rather than only fixing the datastore
  write for the new remote case, move ownership of the terminal status
  write from the running job to the JobServer for *all* job types (local
  included) -- reusing ``_read_job_data_state()``/``_finalize_job_status()``
  (built in Phase 2/3 for SLURM reconciliation only) as the sole path,
  and deleting ``exec_flowchart.py``'s direct-sqlite-write branch
  entirely. Full design (stager abstraction paired with the SSH
  transport, new ``remote_root``/``remote_conda_env`` ini keys,
  stage-out-failure-is-not-job-failure, suggested build order) written up
  above. Not started.
- **2026-08-08 (Phase 8, implemented)** -- Built all four sub-steps in one
  session: (1) moved terminal datastore-status ownership from the job to
  the JobServer for local and SLURM modes alike, deleting
  ``exec_flowchart.py``'s direct sqlite write; (2) ``seamm_slurm.stage``
  (``LocalStager``/``RsyncStager``) plus ``remote_root``/
  ``remote_conda_env``/``remote_run_from_jobserver`` ini keys; (3) wired
  staging into ``seamm_jobserver`` (``_start_job_slurm`` now stages then
  builds the command from the *effective* wdir; ``_finalize_or_resubmit_
  slurm_job`` stages results back before trusting ``job_data.json``,
  treating a stage-out failure as retry-worthy, not job failure); (4) a
  real, unmocked live run from this Mac to molssi10 -- isolated test
  setup, real ``ssh``/``rsync``, a real ``sbatch`` job (``sacct``
  confirmed ``COMPLETED``), a real flowchart that actually ran on
  molssi10's hardware (confirmed via its CPU info in the pulled-back
  ``job_data.json``), and correct local finalization by the JobServer
  itself. 61 new/changed tests total across the three packages (all
  passing), ``make lint`` clean in all three. Test trees removed after
  the run; ``~/SEAMM_DEV/Mac.ini`` updated to the validated config but
  left deliberately inert (points at a stale shared test env, not
  something to actually dispatch real dev work to yet).
- **2026-08-08 (Phase 7 + 8, committed/pushed/PRs open)** -- All three
  packages' working-tree changes committed to their ``dev`` branches
  (Phase 7 and Phase 8 bundled into one ``seamm_jobserver`` commit, per
  Paul's call -- simpler than splitting an already-interleaved diff) and
  pushed. Release prep (HISTORY entries, pre-flight checks, user-guide
  review/update per the release skill) done for all three; the
  ``seamm_jobserver`` user guide's "SLURM submission" section corrected
  in the process -- it previously claimed a job's own process writes its
  final status directly (no longer true, see Phase 8 sub-step 1) and
  that ``transport = ssh`` already worked with no shared filesystem (it
  didn't -- only the script-over-stdin submission mechanism did; nothing
  staged files or built a remote-usable command line before this PR).
  PRs open for ``seamm`` review:
  `seamm_jobserver #18 <https://github.com/molssi-seamm/seamm_jobserver/pull/18>`_,
  `seamm_slurm #4 <https://github.com/molssi-seamm/seamm_slurm/pull/4>`_,
  `seamm_exec #31 <https://github.com/molssi-seamm/seamm_exec/pull/31>`_.
  Not merged or released -- that's ``seamm``'s manual step.
- **2026-08-08 (all three merged and released, campaign complete)** -- All
  three PRs merged by ``seamm`` within the same session. ``seamm_slurm``
  and ``seamm_exec`` released as ``2026.8.8`` first (both confirmed live
  on PyPI, local checkouts synced via ``make update``); ``seamm_jobserver``
  had to wait, since its ``requirements.txt`` has no version pin on
  ``seamm_slurm`` and its own tests import ``seamm_slurm.stage`` for
  real (not mocked) -- CI would have pulled the old PyPI release lacking
  that module otherwise. Once ``seamm_slurm`` was confirmed live,
  ``seamm_jobserver``'s CI passed and it was merged; released as
  ``2026.8.8`` too. Local ``seamm_jobserver`` checkout synced via
  ``make update`` immediately after the GitHub Release/tag was created,
  without waiting for the PyPI publish step of ``Release.yaml`` to finish
  -- ``make update`` rebuilds from the local git checkout at the tagged
  commit, it does not install from PyPI, so only the tag needs to exist.
  **Phase 8 (and the bundled Phase 7) are now fully shipped**: all three
  packages at ``2026.8.8``, ``dev == main`` in all three local checkouts.
