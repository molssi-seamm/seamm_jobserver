2026-08-05 -- SLURM submission for JobServer
=============================================

Status: Phases 0-4 done (SLURM version/JSON groundwork, the ``seamm_slurm``
backend library, JobServer's SLURM mode, real end-to-end validation on
MolSSI10, and this documentation). Phase 5 (a future ``seamm_exec`` ``Slurm``
executor for per-step submission) is not started and not currently
scheduled. Originally tracked as a workspace-root living planning doc; moved
here once the campaign was substantially complete.

Open pull requests from this campaign, all awaiting human review (none
merged yet): `seamm_jobserver #17
<https://github.com/molssi-seamm/seamm_jobserver/pull/17>`_ (the SLURM
mode itself, its docs, and this campaign doc), `seamm_slurm #1
<https://github.com/molssi-seamm/seamm_slurm/pull/1>`_ (docs), and
`seamm_exec #30 <https://github.com/molssi-seamm/seamm_exec/pull/30>`_
(the ``job_data.json`` header bugfix).

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
