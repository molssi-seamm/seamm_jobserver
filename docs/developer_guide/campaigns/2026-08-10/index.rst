2026-08-10 -- Multi-queue job routing (one Dashboard/JobServer, many clusters)
================================================================================

Status: Design/requirements discussion with Paul complete. Phase 1
(``seamm_slurm``: ``type=local`` section type + ``list_sections()``) done.
Phase 2 (``seamm_jobserver``: per-job routing across multiple queues) done.
Phase 3 (``seamm_webui``: ``GET /api/queues``) done. Phase 4
(``seamm_dashboard_client`` + ``seamm/tk_job_handler.py``: the actual
user-facing queue picker) done. Phase 5 (real end-to-end validation) done,
including live redeployment to Paul's own ``~/SEAMM_DEV`` dev
JobServer/webui (both restarted, now running this campaign's code for
real) with two live queues, ``local`` and a real ``molssi10`` SLURM
queue -- both validated with real, independently-``sacct``-confirmed jobs.
This campaign is the direct continuation of
`2026-08-05 -- SLURM submission for JobServer
<../2026-08-05/index.rst>`_: that campaign built the mechanism (a
``<root>/<jobserver-name>.ini`` with one section per cluster/queue target,
each with its own transport/limits/stager) but deliberately left **per-job
routing across sections** as "an open question... not needed for initial
rollout." This campaign answers that question and wires routing all the way
through to the actual submission UI.

Why
---

Today one Dashboard + one JobServer, sharing one datastore, can only send
jobs to whatever single cluster/queue that JobServer instance is configured
for (local subprocess, or the one SLURM section ``load_slurm_config()``
resolves at startup -- see ``jobserver.py:797``). That's a strong
restriction: a user can't submit some jobs to their laptop, some to
TinkerCliffs, some to a lab cluster like "Owl", from one Dashboard. Paul
wants a queue concept, selected per job, the way a user would pick "local" /
"TinkerCliffs" / "Owl" from a dropdown when submitting.

The ``2026-08-05`` campaign already built almost everything needed at the
config layer -- multiple named sections in one ini file, each with its own
transport (``local``/``ssh``), submission defaults, ``.limits``-bounded
per-job overrides, and (Phase 8) a stager for no-shared-filesystem dispatch.
What's missing is: (1) actually dispatching a given job to the section it
asked for, instead of the one section loaded at startup, and (2) a way for
a user to ask for a specific queue at all -- nothing today surfaces "which
queues exist" or ".limits" to any submission path.

Where this needs to land, and why
----------------------------------

Investigated the real submission path before designing the UI piece, rather
than assuming ``seamm_webui`` (which has no job-submission page yet). Real
jobs go through ``seamm/tk_job_handler.py``'s ``TkJobHandler``, the Tk
desktop submit dialog: ``submit_with_dialog()`` builds a dialog with
Dashboard/Project pickers and a dynamically-built parameters table (one row
per flowchart control-parameter, reusing ``sw.LabeledCombobox``/bounded
entries/file pickers), then calls
``dashboard.submit(flowchart, values=value, **result)``
(``seamm_dashboard_client.dashboard.Dashboard.submit()``, which POSTs
``{"cmdline": ..., "control parameters": ...}`` inside ``parameters`` to
``POST /api/jobs``). This is the actual, current, only submission path in
regular use -- not a Jinja HTML form, not a not-yet-built webui page. So the
queue picker and ``.limits``-driven override widgets have to be added to
*this* dialog, not designed only for some future web submission page.

**``seamm_dashboard`` gets no changes at all.** Paul: it's being phased out
in favor of ``seamm_webui``, and he's no longer confident he can cut another
release of it. This *reopens* a narrower reading of the ``2026-08-05``
campaign's "seamm_dashboard won't get this feature" call (which was about
not extending its Jinja/HTML submission UI) -- here it's simpler still: no
new code of any kind in ``seamm_dashboard``, full stop. Practical
consequence: **queue routing is only available for jobs submitted through a
``seamm_webui``-backed Dashboard.** Any Dashboard still running old
``seamm_dashboard`` has no ``/api/queues`` endpoint, and
``tk_job_handler.py`` has to treat that as "this dashboard doesn't offer
queues" (hide the picker) rather than an error -- a mix of upgraded and
not-yet-upgraded dashboards is the expected state during the
``seamm_dashboard`` -> ``seamm_webui`` migration, not an edge case.

Checked ``seamm_webui``'s existing REST surface before designing against
it: ``routers/jobs.py``'s ``submit_job`` already accepts an arbitrary
``parameters: dict`` (``SubmitJobModel.parameters``) and passes it straight
through to ``Job.create(...)`` unchanged. So a ``queue``/``slurm`` key
riding inside ``parameters`` needs **zero** submit-side change in
``seamm_webui`` -- only a new *read* endpoint (``GET /api/queues`` or
similar, mirroring ``routers/projects.py``'s ``list_projects``) is needed,
to advertise which queues exist and their ``.limits``. ``seamm_webui``
already has ``--root`` wired at startup (``main.py:127``, currently used
for SSL cert storage), the same ``root`` ``seamm_slurm.config`` needs to
find ``<root>/<jobserver-name>.ini``.

Guiding decisions (locked in)
-------------------------------

- **One JobServer instance, multiple ini sections, routed per job** -- not
  multiple JobServer processes racing against one shared datastore.
  ``check_for_new_jobs()`` (``jobserver.py:467``) has **no claiming/locking**
  today (a plain ``SELECT ... WHERE status = 'submitted'``), so concurrent
  JobServer instances polling the same datastore would double-start jobs.
  A single instance with multiple sections sidesteps that problem entirely
  rather than requiring new locking logic. Running a second, independent
  JobServer (e.g. a cluster's own resident instance, as MolSSI10 and ChemAI
  already do for their own local users) remains a valid, separate
  deployment pattern -- just not what this campaign is solving for.
- **``type = local`` becomes a real section type**, alongside the existing
  (and, in practice, only-implemented-so-far) ``type = slurm``. Today
  ``SlurmSection.type`` is parsed but never branched on --
  ``build_backend()``/``build_stager()`` unconditionally build SLURM
  objects. A no-scheduler "just run it as a local subprocess" queue becomes
  a normal section in the dispatch table instead of the current
  special-cased "no ini file at all" path -- so ``local``, ``tinkercliffs``,
  and ``owl`` are uniformly just named sections a job can request.
- **No changes to ``seamm_dashboard``**, per the section above. All new
  server-side code goes in ``seamm_webui``.
- **The Tk desktop dialog (``seamm/tk_job_handler.py``) is the primary UI
  target**, not a web page, per the section above.
- **Reuse everything the ``2026-08-05`` campaign already built**: the
  multi-section ini format, ``SlurmSection.limits``/``merge_overrides()``
  (already validates/bounds overrides server-side, already the mechanism
  the webui rewrite plan reserved a ``parameters["slurm"]`` slot for), and
  the ``LocalSlurm``/``SshSlurm`` + ``LocalStager``/``RsyncStager`` pairing.
  This campaign is routing and surfacing, not rebuilding the SLURM
  integration.

Open questions (not yet locked in -- Paul's call)
----------------------------------------------------

- **Field naming.** Recommend a top-level ``parameters["queue"]`` (e.g.
  ``"tinkercliffs"``), scheduler-agnostic since ``local`` isn't a SLURM
  concept, with per-job SLURM resource overrides staying in the sibling
  ``parameters["slurm"]`` dict the ``2026-08-05`` campaign already defined
  (``{"ntasks": 4, "mem": "40G"}``). This is a small deviation from the
  placeholder the ``seamm_webui`` rewrite plan doc sketched
  (``parameters["slurm"]["section"]``) -- that placeholder was written
  before this design existed and explicitly said "not needed yet, just
  don't let the shape preclude it later," so nothing currently depends on
  the nested form. Not yet confirmed with Paul.
- **Default queue when a job doesn't request one.** Proposed: keep today's
  ``[DEFAULT] default =`` key in ``<root>/<jobserver-name>.ini`` as the
  fallback, same as the single-section case works today.
- **Exact new ``seamm_webui`` route shape** (``GET /api/queues`` vs. nested
  elsewhere) and how ``seamm_webui`` learns which JobServer ``--name`` to
  read a config for, if a ``root`` ever hosts more than one JobServer
  instance (today's default is the hostname, matching
  ``seamm_jobserver``'s own default). Not expected to be a hard problem,
  just not pinned down yet.

Architecture
------------

- **``seamm_slurm``**: ``build_backend()``/``build_stager()``
  (``seamm_slurm/config.py``) gain a real ``type == "local"`` branch
  (a trivial local-subprocess backend/no-op stager, alongside the existing
  SLURM ones). A new helper to enumerate *all* sections in a
  ``<root>/<jobserver-name>.ini`` (not just the one ``load_slurm_config()``
  resolves via ``default=``), returning name + ``.limits`` only -- safe for
  a lightweight consumer (``seamm_webui``, eventually the Tk client
  indirectly) to serialize into an API response without leaking
  ``host``/transport secrets.
- **``seamm_jobserver``**: ``start()`` (``jobserver.py:797``) loads *all*
  sections into ``self._slurm`` (a ``{name: SlurmSection}`` dict, plus
  per-section built backend/stager) instead of one. ``check_for_new_jobs()``
  (``jobserver.py:467``) reads each submitted job's requested queue,
  validates it against known section names (unknown queue -> ``startup
  error``, reusing the exact error path ``merge_overrides()`` already
  raises into today), and dispatches to that section. ``max_concurrent_jobs``
  and ``poll_many()`` batching both become per-section rather than
  instance-global. Startup reattachment records/recomputes each tracked
  job's queue name (not just its ``slurm_job_id``) so restart knows which
  backend to poll -- same pattern Phase 8 already used for ``remote_wdir``.
- **``seamm_webui``**: new ``GET /api/queues``-shaped route (mirrors
  ``routers/projects.py``'s ``list_projects``), reading
  ``seamm_slurm.config``'s new enumerate-sections helper against the
  server's own ``--root``. No change needed to ``submit_job`` itself --
  ``parameters`` already passes through untouched.
- **``seamm_dashboard_client``**: new ``Dashboard.list_queues()`` (mirrors
  ``list_projects()``, ``dashboard.py:175``), returning ``[]``/``None`` on a
  404 rather than raising, so callers can treat "no queues" as an ordinary
  case, not an error. ``Dashboard.submit()`` (``dashboard.py:431``) gains
  ``queue=``/``slurm_overrides=`` kwargs, written into
  ``data["parameters"]`` alongside the existing ``cmdline``/``control
  parameters``.
- **``seamm/tk_job_handler.py``**: a new Queue ``sw.LabeledCombobox`` in
  ``create_submit_dialog()``, next to the existing Dashboard/Project
  pickers, populated in ``dashboard_cb()`` (``tk_job_handler.py:287``) right
  where projects are already fetched -- only shown when
  ``list_queues()`` returns something. A ``.limits``-driven override block
  reuses the exact widget-building pattern ``submit_with_dialog()`` already
  uses for flowchart control-parameters (``ScrolledColumns``: dropdown for
  ``.choices``, bounded entry for ``.min``/``.max``, nothing rendered for a
  field absent from ``overridable``). ``handle_dialog()`` folds the chosen
  queue/overrides into ``result`` alongside ``project``/``title``/
  ``description``.

Phased plan
-----------

Phase 1 -- ``seamm_slurm``: local section type + enumeration (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``SlurmSection`` gained a ``type`` field (default ``"slurm"``, so every
existing ini file/section behaves identically unless it opts in). A new
``[section]`` key ``type = local`` is recognized and validated at parse
time (``_build_section()``'s ``_VALID_TYPES`` check, alongside the existing
per-section parsing shared by ``load_slurm_config()`` and the new
``list_sections()`` below) -- an unrecognized ``type`` raises clearly rather
than silently falling through to SLURM behavior. ``build_backend()``/
``build_stager()`` both raise immediately for a ``type=local`` section
(rather than building a SLURM object from stray ``transport`` defaults) --
by design, a local-type section has no SLURM backend/stager to build at
all; ``seamm_jobserver`` (Phase 2) routes it through the existing
local-subprocess path instead, unchanged. New ``list_sections(root,
jobserver_name) -> dict[str, SlurmSection]`` enumerates *every* section in
a config file (not just the one ``default=``/single-section resolves),
sharing the same per-section parsing as ``load_slurm_config()`` via a new
``_build_section()`` helper -- returns ``{}`` if the ini file doesn't
exist, mirroring ``load_slurm_config()``'s ``None``-if-missing convention.
This is what Phase 2 (per-job routing) and Phase 3 (``seamm_webui``'s
``GET /api/queues``) will consume. 8 new tests (103 total in
``seamm_slurm``), ``make format lint install test`` clean, installed into
``seamm-dev``.

Phase 2 -- ``seamm_jobserver``: per-job routing (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New ``_load_queue_config(root)`` (split out of ``initialize()`` so it's
unit-testable on its own) loads *every* section via
``seamm_slurm.list_sections()`` into ``self._sections`` (was: one
``SlurmSection`` via ``load_slurm_config()``), builds a backend/stager per
``type = slurm`` section into ``self._backends``/``self._stagers`` (keyed
by section name; ``type = local`` sections get neither), and records
``self._default_queue`` from ``load_slurm_config()``'s existing
``default=``/single-section resolution (unchanged logic, just also kept
alongside the full section map now).

``check_for_new_jobs()`` reads each submitted job's ``parameters["queue"]``,
falls back to ``self._default_queue`` if absent, rejects (``startup
error``) an unknown queue or a missing queue with no configured default,
and only then dispatches -- per-queue ``max_concurrent_jobs`` are checked
independently, so one full queue never blocks another from accepting new
jobs. A queue's ``type`` (not a single instance-wide mode any more)
decides local-subprocess vs. SLURM dispatch, so a single instance can now
genuinely run a ``type = local`` queue and one or more ``type = slurm``
queues at the same time -- ``check_for_finished_jobs()`` always runs both
the local-mode and SLURM-mode polling passes each cycle (filtered by each
tracked job's ``mode``) rather than choosing one based on instance-wide
config, and the SLURM pass groups tracked jobs by queue for one batched
``poll_many()`` call per distinct backend, keeping two clusters' SLURM job
IDs from ever being compared in the same lookup table (two different
clusters can coincidentally reuse the same job id number).

Every tracked job dict gains a ``"queue"`` key (set explicitly by
``_start_job_slurm``/``_start_job_local``/reattachment); every place that
reads it uses a new ``_resolve_queue(data)`` helper
(``data.get("queue") or self._default_queue``) rather than the raw key
directly -- both a genuine safety net for a row that predates this
campaign (before per-job routing existed, a JobServer instance could only
ever have had the one section that is now its default) and, as a side
effect, what let nearly all of the pre-existing single-queue test suite
(58 tests) keep passing completely unchanged. ``_start_job_slurm`` now
persists the resolved ``queue`` into the datastore row's ``parameters``
(alongside ``slurm_job_id``/``resubmit_count``) for auditability and so a
restart's reattachment doesn't have to guess. ``_reattach_slurm_jobs``
groups candidate rows by resolved queue the same way the finished-jobs
poll does, and leaves a row whose recorded queue isn't a currently-
configured ``type = slurm`` queue alone (logged, not guessed at) rather
than reconciling it against the wrong backend.

Entirely additive, same guarantee as the 2026-08-05 campaign: no
``<root>/<jobserver-name>.ini`` at all means ``self._sections`` stays
empty and ``check_for_new_jobs()`` takes a dedicated
``_check_for_new_jobs_unmanaged()`` path that is the exact original
pre-multi-queue code (every submitted job runs immediately as an uncapped
local subprocess, ``parameters["queue"]`` ignored entirely if present).

11 new tests (69 total) covering genuine multi-queue behavior: routing to
an explicitly-requested queue, falling back to the default, rejecting an
unknown/missing-with-no-default queue, independent per-queue concurrency
caps, a ``type = local`` queue and ``type = slurm`` queues coexisting and
both being polled in one ``check_for_finished_jobs()`` cycle, per-queue
``poll_many()`` batching not cross-contaminating two queues' same-numbered
SLURM job ids, reattachment routing by each row's own recorded queue, an
unroutable recorded queue being left alone rather than guessed at, and
``_load_queue_config()`` itself against a real two-section (one
``type=local``, one ``type=slurm``) ini file. ``make format lint install
test`` clean. Not yet committed/released.

Phase 3 -- ``seamm_webui``: ``GET /api/queues`` (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Confirmed before writing any code that ``routers/jobs.py``'s
``submit_job`` already accepts an arbitrary ``parameters: dict`` passed
straight through to ``Job.create()`` -- so no submission-side change was
needed at all, only the new read endpoint.

New ``seamm_webui/queue_config.py`` (mirrors ``db.py``'s pattern: a small
module holding process-wide state set once at startup, read by a router)
holds ``root``/``jobserver_name``, configured by ``main.py``'s
``create_app()`` -- which gained two new optional parameters,
``root: Optional[str] = None`` and ``jobserver_name: Optional[str] =
None``, both defaulting to "don't know," so every pre-existing caller/test
(none of which pass them) is unaffected: the new endpoint just reports no
queues. ``root`` is the same ``--root`` config directory
``<root>/<jobserver-name>.ini`` already lives in for
``seamm_jobserver``/``seamm_slurm`` -- **not** ``create_app()``'s
``datastore_dir``, mirroring the pre-existing ``--root`` vs ``--datastore``
distinction ``db.py`` already documents (a real bug during that package's
own scaffolding). ``run()`` gained a new ``--jobserver-name`` CLI flag
(default: this host's hostname, matching ``seamm_jobserver``'s own
``--name`` default) for the case where a host runs more than one
independent JobServer instance and the pairing needs to be explicit.

New ``seamm_webui/routers/queues.py``: ``GET /api/queues`` (mirrors
``routers/projects.py``'s ``list_projects`` -- same ``require_permission
("read")`` gating, since by the time a client would call this it's already
authenticated the same way ``list_projects`` requires). Calls
``seamm_slurm.config.list_sections()`` against the configured
``root``/``jobserver_name``; for each section returns ``name``, ``type``,
``default`` (whether ``load_slurm_config()``'s own
``default=``/single-section resolution picked it), and ``limits``
(``FieldLimits.choices``/``minimum``/``maximum``, serialized as-is) --
**never** ``transport``/``host``/``remote_root``/``remote_conda_env``, all
of which are this host's own system/machine config, not something a
submitting client needs or should see. An ambiguous config (multiple
sections, no ``[DEFAULT] default=``) -- which crashes ``seamm_jobserver``
itself at startup by design -- is caught here and degrades to "no queue
marked default" rather than 500ing the whole listing, since this is a
read-only status endpoint, not the enforcement point (the JobServer
re-validates every request server-side regardless of what any client
believes the queues/limits are, unchanged from Phase 6 of the 2026-08-05
campaign).

Added ``seamm-slurm`` to ``pyproject.toml``'s ``dependencies``.

Note found and confirmed **not** to be a regression: the existing
``tests/test_auth.py::test_new_local_account_can_log_in`` fails on a clean
``dev`` checkout too (an unrelated ``IntegrityError`` in
``seamm_datastore``'s role-assignment code, reproduced via a
``git stash``/``git stash pop`` round-trip before writing any Phase 3
code) -- not touched or investigated further here, out of scope for this
campaign.

6 new tests (``tests/test_queues.py``): no-``root``/no-ini-file both
return ``[]``, real two-section (one ``type=local``, one ``type=slurm``
with ``.limits``) listing including the resolved default, host/transport/
remote_* never present in the response, ``--jobserver-name`` defaulting to
hostname, and the ambiguous-default case not crashing. ``make format
lint`` clean; full suite 24 passed / 1 pre-existing failure (unrelated,
see above).

Phase 4 -- ``seamm_dashboard_client`` + ``seamm/tk_job_handler.py``: the user-facing UI (done)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Addendum to Phase 3, discovered while building its actual consumer**:
the ``GET /api/queues`` payload gained a ``"current"`` value per
overridable field (``routers/queues.py``'s ``_serialize_limits()``, now
also takes the queue's ``directives``) -- a queue's own site-default
directive value (e.g. ``ntasks``'s current ``1``), not a secret, and
needed so the override UI can show what's actually being overridden
("current: 1, choose up to 6") instead of an unexplained blank field.
Updated ``seamm_webui``'s tests to match.

``seamm_dashboard_client.Dashboard`` gained ``list_queues()`` (mirrors
``list_projects()``'s shape, hitting ``GET /api/queues`` -- only
``seamm_webui`` implements this, so a non-200 response, including a plain
404 from an old ``seamm_dashboard``, is logged at ``debug`` and treated as
"no queues," not an error) and ``submit()`` gained ``queue=``/
``slurm_overrides=`` keyword arguments, folded into
``data["parameters"]["queue"]``/``["slurm"]`` only when given (omitted
entirely otherwise, so an un-updated caller's POST body is byte-identical
to before). 5 new tests (``responses``-mocked HTTP, including a fake
minimal flowchart object exercising ``submit()`` without needing a real
``seamm.Flowchart``) -- 10 total, ``make format lint install test`` clean.

``seamm/tk_job_handler.py`` (the actual, only-in-real-use submission
dialog): new Queue ``sw.LabeledCombobox`` between Project and Title,
populated by a new ``update_queues()`` (called from ``dashboard_cb()``,
the same place projects are already fetched) -- hidden entirely
(``grid_forget()``, per CLAUDE.md's "hide, don't disable" GUI principle)
when the current dashboard has no queues at all, defaulting to whichever
queue ``list_queues()`` marks ``"default"`` otherwise. A new
``.limits``-driven "Queue options" table (``build_queue_overrides()``,
rebuilt on every queue-selection change via a new ``queue_cb()``) renders
a readonly dropdown for a field with ``choices``, a plain entry otherwise,
with the field's current site default and min/max shown as a hint in the
Description column -- reuses the dialog's existing
``ScrolledColumns``-table idiom (the same one already used for flowchart
control-parameters), not a new widget pattern. A field left blank means
"don't override it," mirroring the ini file's own "blank means don't pass
that directive" convention (``get_queue_overrides()``). ``handle_dialog()``
and ``submit_with_dialog()`` thread the chosen queue and overrides through
to ``Dashboard.submit()`` alongside the pre-existing
project/title/description. Dialog rows renumbered throughout to make room
(0 dashboard, 1 project, 2 queue, 3 title, 4-5 description, 6 reset
buttons, 7-8 queue overrides, 9-10 flowchart parameters) -- both new
sections are gridded/forgotten dynamically, matching how the flowchart
parameters section already worked, not statically placed.

This module had **zero pre-existing automated tests** (a Tk GUI, not
previously covered) and no browser-automation-equivalent tool exists for a
native Tk app in this environment, so real coverage came from a manual,
scripted smoke test instead of a checked-in test suite: created a real
(headless, ``root.withdraw()``'d) ``Tk()`` root, built the dialog, and
drove ``update_queues()``/``build_queue_overrides()``/
``get_queue_overrides()`` against fake dashboard objects returning
multi-queue, single-queue-with-no-limits, and zero-queue responses --
confirmed the picker populates/defaults/hides correctly, the overrides
table rebuilds correctly on queue-switch, and a filled-in override is read
back correctly. Re-run clean after ``make install``. ``make format lint``
clean (28 files unchanged/reformatted); ``pytest tests/`` (22 pre-existing,
unrelated tests) still green.

Phase 5 -- validation (local two-queue done; remote SLURM queue not started)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Local two-queue validation (done).** A fully isolated throwaway setup
(matching the ``2026-08-05`` campaign's own discipline for real-system
tests), entirely under the session scratchpad, never touching any of
Paul's real ``~/SEAMM``/``~/SEAMM_DEV`` roots or their live
``seamm-jobserver``/``seamm-webui`` processes (explicitly confirmed still
running, untouched, after this test's own instances were stopped):

- A throwaway ``<root>`` with a fresh datastore and a
  ``phase5test.ini`` defining two ``type = local`` queues, ``fast``
  (``max_concurrent_jobs = 5``, the ``[DEFAULT] default``) and ``slow``
  (``max_concurrent_jobs = 1``).
- A real ``seamm-webui`` process (``--root``/``--datastore`` pointed at
  the throwaway tree, ``--jobserver-name phase5test``, ``--auth none``,
  loopback) -- confirmed for real that ``GET /api/queues`` returns both
  queues, correctly marking ``fast`` as the default, straight from the
  real ini file through the real HTTP stack.
- A real ``seamm-jobserver`` process against the same throwaway root/
  datastore -- its startup log confirmed ``_load_queue_config()`` really
  discovering both queues ("Configured queues: ['fast', 'slow']
  (default='fast')").
- A real, unmocked ``seamm_dashboard_client.Dashboard`` (no
  username/password -- exercises the exact "none"-auth, no-token
  ``login()`` short-circuit a loopback Tk-client session would use)
  submitting two real jobs, one per queue, using a real
  ``seamm.Flowchart`` (``StartNode -> FromSMILESStep``, built via the
  ``seamm`` API rather than a hand-authored ``.flow`` file -- mirrors the
  ``2026-08-05`` campaign's own Phase 8 validation, and deliberately
  avoids the ``seamm_datastore``-bundled sample flowchart, whose DFTB+
  step failed with an unrelated, pre-existing ``dftbplus_step``
  version-skew bug (an unrecognized stored optimization-method string) --
  confirmed via that job's real traceback, nothing to do with routing,
  not investigated further as out of scope here).
- **Confirmed, from the real datastore, not inference**: both jobs ran as
  genuine local subprocesses, finished successfully, and each job's own
  ``parameters["queue"]`` in the database matches exactly the queue it was
  submitted to (job 3 -> ``fast``, job 4 -> ``slow``) -- the core guarantee
  this phase set out to prove, for real, end to end, through every layer
  built in Phases 1-4.

**Live redeployment to ``~/SEAMM_DEV`` + real MolSSI10 validation (done,
2026-08-10, Paul's explicit go-ahead: "Fine to update both the local DEV
server and MolSSI10 -- that way both you and I can test both").**

A real, independent (and unrelated to this campaign) bug was found while
setting this up: ``~/SEAMM_DEV``'s launchd-managed JobServer
(``org.molssi.seamm.dev_jobserver``, no ``--name`` in its plist, so it
defaults to ``socket.gethostname()``) had been silently running in plain
local-subprocess mode this entire time -- ``~/SEAMM_DEV/Mac.ini`` (written
during the ``2026-08-05`` campaign's Phase 8, "--name Mac, the hostname")
was orphaned at some point after this Mac's hostname changed to
``PaulsPersonal.local``, so no ini file ever matched and the SLURM/
ssh-transport dispatch it documented as "validated working" and "ready
for real dev use" had not actually been in effect. Confirmed via
``hostname``/``socket.gethostname()`` both reporting
``PaulsPersonal.local``, and empty/stale ``jobserver.log``/
``jobserver_status.json`` showing no SLURM activity. Fixed by adding a
new ``~/SEAMM_DEV/PaulsPersonal.local.ini`` (``Mac.ini`` left in place,
unused, for history) with the same ``[molssi10]`` section plus a new
``[local]`` (``type = local``) queue -- ``default = molssi10`` kept
unchanged, so this is purely additive.

Deployment steps, each checked before acting (mirroring the
``2026-08-05`` campaign's own discipline for touching real systems):

- Confirmed ``squeue -u psaxe`` on molssi10 was empty, both before and
  immediately before submitting.
- Confirmed the ``seamm-dev`` conda env (what
  ``org.molssi.seamm.dev_jobserver`` actually runs from) already had this
  campaign's Phases 1-2 code installed and identical to the working tree
  (``diff`` against the installed ``jobserver.py``) -- no reinstall
  needed there, only a restart.
- Found the separate, **not** launchd-managed ``seamm-webui`` process
  Paul had already been running by hand on port 8010 was (a) missing
  ``--root`` entirely (defaulting to ``~/SEAMM``, the wrong root for
  finding ``PaulsPersonal.local.ini``) and (b) running a stale
  ``seamm_webui``/``seamm_slurm`` install in its own ``.venv`` predating
  this campaign entirely (confirmed via ``pip show``/``diff`` against
  ``main.py``) -- its own ``pip install .`` had even pulled a *released*
  ``seamm-slurm`` from PyPI as a transitive dependency, silently
  overwriting the dev install with a version lacking ``list_sections()``/
  ``type=local`` entirely. Fixed: reinstalled this checkout's
  ``seamm_webui`` *and* ``seamm_slurm`` into that ``.venv``, then
  relaunched with ``--root ~/SEAMM_DEV --datastore ~/SEAMM_DEV/Jobs
  --port 8010`` (unchanged otherwise).
- Restarted ``org.molssi.seamm.dev_jobserver`` via ``launchctl kickstart
  -k gui/<uid>/...`` (matching the ``2026-08-05`` campaign's exact
  precedent for this launchd-managed process) -- not a manual kill/relaunch.
- **Left completely untouched**: MolSSI10's own resident
  JobServer/Dashboard (no code changes needed there at all -- ssh-transport
  dispatch only requires a working ``run_from_jobserver`` on the remote
  end, which already existed; all the new routing logic runs entirely on
  the dispatching side), the old ``seamm-dashboard`` Flask app still
  running as ``org.molssi.seamm.dev_dashboard`` on port 55066 (per the
  standing "no changes to seamm_dashboard" decision), and Paul's
  production (non-dev) ``~/SEAMM`` JobServer/Dashboard.

**Real submission results**, via ``Dashboard`` (no credentials) against
the now-restarted ``http://127.0.0.1:8010``, one ``FromSMILESStep`` job
per queue:

- ``queue=local`` -> job 3885, finished, ``parameters["queue"] ==
  "local"``, no ``slurm_job_id`` (ran as a genuine local subprocess).
- ``queue=molssi10`` -> job 3886, finished, ``parameters["queue"] ==
  "molssi10"``, real ``slurm_job_id = 28``. **Independently confirmed via
  ``sacct -j 28`` on molssi10 itself** (not just trusting the local
  datastore): ``28|seamm-3886|COMPLETED|0:0``.
- Cleaned up only this test's own remote scratch directory
  (``/home/psaxe/seamm_dev_remote_jobs/Job_003886``) afterward, leaving
  Paul's pre-existing ``Job_003882``/``Job_003883`` there untouched. Left
  the two local datastore job rows in place (harmless, clearly titled
  "phase5 dev-server validation (...)") as a visible record Paul can see
  directly when he goes to test this himself.

Status log
----------

- **2026-08-10** -- Requirements/architecture discussion with Paul.
  Reviewed the ``2026-08-05`` campaign's existing multi-section ini design
  and confirmed it was never wired up for per-job routing (single section
  loaded at startup, no claiming/locking in ``check_for_new_jobs()``).
  Traced the real job-submission path (``seamm/tk_job_handler.py`` ->
  ``seamm_dashboard_client`` -> ``POST /api/jobs`` on whatever Dashboard is
  configured) rather than assuming a web submission page. Locked in:
  single JobServer instance with multi-section per-job routing (not
  multiple racing instances), ``type = local`` as a real section type, and
  -- after Paul clarified ``seamm_dashboard`` is being phased out for
  ``seamm_webui`` and may not be releasable again -- **no changes to
  ``seamm_dashboard`` at all**, narrower than the ``2026-08-05`` campaign's
  original "no Jinja UI" reading. Confirmed ``seamm_webui``'s
  ``submit_job`` already passes ``parameters`` through untouched, so only a
  new read endpoint is needed there. Field naming
  (``parameters["queue"]`` vs. the webui rewrite plan's earlier
  ``parameters["slurm"]["section"]`` placeholder) and the default-queue
  fallback left as open questions for Paul, not yet confirmed. Plan
  written; implementation not started.
- **2026-08-10 (Phase 1)** -- Built the ``seamm_slurm`` side: ``type`` field
  on ``SlurmSection`` (default ``"slurm"``, fully backward compatible),
  ``type = local`` recognized/validated at parse time,
  ``build_backend()``/``build_stager()`` raise clearly for a ``type=local``
  section instead of building a stray SLURM object, and a new
  ``list_sections()`` enumerating every section in a config file (not just
  the resolved default one) for the routing/UI work still to come. 8 new
  tests (103 total), ``make format lint install test`` clean. Not yet
  committed/released -- installed into ``seamm-dev`` only. Phases 2-5 not
  started.
- **2026-08-10 (Phase 2)** -- Built the ``seamm_jobserver`` side: multi-
  section loading (``_load_queue_config()``), per-job ``queue`` resolution
  and validation in ``check_for_new_jobs()``, per-queue concurrency caps
  and ``poll_many()`` batching, a ``type = local`` queue and ``type =
  slurm`` queues able to coexist on one instance, and reattachment routing
  by each row's own recorded queue. Preserved the "no ini file -> zero
  behavior change" guarantee via a dedicated
  ``_check_for_new_jobs_unmanaged()`` path, and a ``_resolve_queue()``
  fallback-to-default helper that both handles genuinely pre-existing rows
  and kept the entire prior single-queue test suite passing unmodified. 11
  new tests (69 total), ``make format lint install test`` clean. Not yet
  committed/released -- installed into ``seamm-dev`` only. Phases 3-5 not
  started.
- **2026-08-10 (Phase 3)** -- Built the ``seamm_webui`` side: confirmed its
  job-submission route already passes ``parameters`` through untouched, so
  only a new read endpoint was needed. New ``queue_config.py`` +
  ``routers/queues.py`` (``GET /api/queues``), two new optional
  ``create_app()`` parameters (``root``/``jobserver_name``, both
  backward-compatible no-ops when omitted), a new ``--jobserver-name`` CLI
  flag, and ``seamm-slurm`` added as a dependency. Found (and confirmed,
  not caused) a pre-existing unrelated test failure via a
  stash/stash-pop round-trip against a clean checkout. 6 new tests, ``make
  format lint`` clean. Not yet committed/released. Phases 4-5 not started.
- **2026-08-10 (Phase 4)** -- Built the user-facing piece: a small
  ``seamm_webui`` addendum (a ``"current"`` value per overridable field,
  needed once actually building the UI that consumes it), then
  ``seamm_dashboard_client.Dashboard.list_queues()``/``submit()`` (10
  tests total), then ``seamm/tk_job_handler.py``'s queue picker +
  ``.limits``-driven override table (dialog rows renumbered to fit). This
  module had no pre-existing tests and no equivalent to browser automation
  exists for a native Tk app here, so validated via a manual headless-Tk
  smoke script instead (multi-queue, no-limits, and zero-queue cases all
  confirmed correct) rather than a checked-in suite. ``make format lint
  install test`` clean in all three packages (22 pre-existing ``seamm``
  tests unaffected). Not yet committed/released. Phase 5 not started.
- **2026-08-10 (Phase 5, local)** -- Real, unmocked, end-to-end validation
  in an isolated throwaway root under the session scratchpad: real
  ``seamm-webui`` + ``seamm-jobserver`` processes against a fresh
  datastore and a two-queue (``fast``/``slow``, both ``type=local``) ini
  file, a real ``Dashboard`` (no credentials, "none"-auth short-circuit)
  submitting two real ``FromSMILESStep`` jobs, one per queue. Both
  finished successfully with their datastore row's recorded queue
  matching exactly what was requested -- confirmed from the real
  datastore. Paul's live ``~/SEAMM``/``~/SEAMM_DEV`` JobServer/webui
  processes confirmed still running, untouched, throughout. Asked Paul
  whether to also validate against a real remote SLURM queue
  (e.g. MolSSI10); he replied "Fine to update both the local DEV server
  and MolSSI10 -- that way both you and I can test both."
- **2026-08-10 (Phase 5, live redeployment + MolSSI10)** -- Per Paul's
  go-ahead above: redeployed for real to ``~/SEAMM_DEV``. Found (unrelated
  to this campaign) that its JobServer had been silently running in
  plain local-subprocess mode for some time -- ``Mac.ini`` orphaned by a
  hostname change to ``PaulsPersonal.local``, so the "validated working"
  SLURM/ssh-transport dispatch it documented had not actually been active.
  Fixed with a new, correctly-named ``PaulsPersonal.local.ini`` (same
  ``[molssi10]`` section plus a new ``[local]`` queue, default unchanged).
  Also found the hand-run dev ``seamm-webui`` process was missing
  ``--root`` and running a stale ``.venv`` install that had even pulled a
  *released* (pre-campaign) ``seamm-slurm`` from PyPI as a dependency --
  fixed by reinstalling this checkout's ``seamm_webui``/``seamm_slurm``
  into that ``.venv`` and relaunching with the correct flags. Restarted
  the JobServer via ``launchctl kickstart`` (matching the ``2026-08-05``
  campaign's own precedent for this launchd-managed process). Left
  MolSSI10's own resident services, the old ``seamm-dashboard`` Flask
  app, and Paul's production (non-dev) JobServer/Dashboard completely
  untouched throughout. Confirmed ``squeue`` empty on molssi10 before
  submitting. Submitted one real job to each of the two live queues:
  ``local`` finished as a genuine local subprocess; ``molssi10`` finished
  with a real ``slurm_job_id``, independently confirmed via ``sacct -j 28``
  on molssi10 itself (``COMPLETED``, exit ``0:0``). Cleaned up only this
  test's own remote scratch directory, leaving Paul's pre-existing ones
  and the local datastore job rows (as a visible record) untouched.
  **This closes out Phase 5 and the campaign's implementation work.**
  Nothing in any of the five touched packages (``seamm_slurm``,
  ``seamm_jobserver``, ``seamm_webui``, ``seamm_dashboard_client``,
  ``seamm``) has been committed or released yet.
