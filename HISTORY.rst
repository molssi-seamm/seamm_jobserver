=======
History
=======
2026.8.13 -- Internal: lock a remote job's files before pulling them back
    * The end-of-run pull of a ``transport = ssh`` job's remote files is
      now guarded by an inter-process file lock in the job's own working
      directory, so it can't run concurrently with another process
      independently pulling the same job's files (e.g. a Dashboard
      syncing a still-running job's files on demand) -- a lock-contention
      failure is treated the same as a transient transfer failure and
      retried next poll cycle.
    * ``_remote_wdir()`` now delegates to ``seamm_slurm``'s new
      ``SlurmSection.remote_wdir_for()`` (requires ``seamm_slurm >=
      2026.8.13``) instead of computing the path inline, so this
      JobServer and any other caller (e.g. a Dashboard) can't compute a
      job's remote path differently.
    * New dependency: ``fasteners`` (already used elsewhere in the SEAMM
      stack, e.g. ``seamm_webui``'s own file-locking).
    * No user-visible behavior change.

2026.8.11 -- Honor a queue's setup= shell commands before running a job
    * A SLURM queue's new ``setup`` directive (see ``seamm_slurm``'s
      ``2026.8.11`` release) is now prepended to the generated sbatch
      script, before ``run_from_jobserver`` runs -- e.g. ``setup = module
      load ORCA`` for a queue whose own submission environment doesn't
      otherwise carry whatever a code's ``installation = modules``
      setting needs. No effect for a queue that doesn't set it.

2026.8.10 -- Route jobs to multiple queues/clusters from one JobServer instance
    * A JobServer instance can now be configured with more than one queue
      (cluster/section) in its ``<root>/<jobserver-name>.ini`` and route
      each job to the one it asks for (``parameters["queue"]``), falling
      back to the instance's default queue if it doesn't specify one.
      Requesting an unknown queue, or no queue with no default configured,
      fails the job immediately (``startup error``) rather than silently
      running it somewhere unintended.
    * Each queue's ``max_concurrent_jobs`` is enforced independently, so
      one full queue never blocks another from accepting new jobs, and
      SLURM polling is batched per queue so two different clusters'
      same-numbered SLURM job ids are never compared against each other.
    * A ``type = local`` queue (see ``seamm_slurm``'s ``2026.8.10``
      release) and one or more ``type = slurm`` queues can now coexist on
      the same JobServer instance -- some jobs run as local subprocesses,
      others dispatch to real SLURM clusters, from one process.
    * A SLURM job's ``job_data.json`` now records which queue it ran on
      and its SLURM job id (found from real usage: nothing previously told
      a user which cluster a finished job had actually run on).
    * No ``<root>/<jobserver-name>.ini`` at all means behavior is
      unchanged from before this feature existed -- every job still runs
      as an uncapped local subprocess.

2026.8.9 -- Bugfix: startup reattachment could misapply SLURM staging to a local job
    * On restart, a job's ``running``/``kill`` datastore row was reattached
      based on whether the JobServer instance is *currently*
      SLURM-configured, not on what that specific job actually is. A local
      job predating SLURM being enabled on this instance (or, worse, one
      still genuinely running) could be treated as a lost SLURM submission:
      at best retrying a pointless remote file transfer forever instead of
      reading its already-correct local status, at worst getting resubmitted
      via SLURM as a real duplicate run of a job still running locally.
      Reattachment now checks each job's own recorded pid or SLURM job id,
      not the instance's current mode, so a local job is always recognized
      as local regardless of whether SLURM is configured.
    * Bugfix: a locally-run job resumed after a restart could raise an
      error the next time it finished, from a missing internal field.
    * A locally-run job whose process ended with no other evidence of its
      outcome now has its actual result read from the job's own record
      rather than being assumed successful.

2026.8.8 -- Support SLURM dispatch to a cluster with no shared filesystem
    * A JobServer can now dispatch jobs via SLURM to a cluster it shares no
      filesystem with -- for example, a laptop reaching a remote cluster
      over ssh. A job's working directory is staged there before
      submission and its results staged back after, automatically.
      Configure it with new ``remote_root`` and
      ``remote_run_from_jobserver`` (or ``remote_conda_env``) options in
      the relevant section of ``<root>/<jobserver-name>.ini``. See the
      User Guide for the full option list.
    * Internal: the JobServer itself, not the running job, now writes a
      job's final status to the datastore, for every job type (this was
      already true for the SLURM-losing-track case; now it's the normal
      path for local jobs too). Fixes a real gap: a local job that
      crashed hard before it could write its own status previously stayed
      stuck as ``running`` forever, with no recovery -- the JobServer now
      finalizes it from the process's exit status instead.
    * Live-validated end to end against a real cluster: a real job staged
      out via rsync, submitted via sbatch, confirmed completed via SLURM's
      own accounting, and staged back correctly.

2026.8.7 -- Actively stop deleted or explicitly-killed jobs
    * Deleting a job (e.g. via the dashboard) removes its row and files, but
      previously left whatever was actually running it untouched -- it just
      ran on until it crashed on its own missing files. The JobServer now
      notices a tracked job's row disappearing and actively stops it
      (``scancel`` in SLURM mode, terminating the process in local mode),
      every poll cycle.
    * A job can also be stopped without deleting it or its files, by setting
      its ``status`` to ``kill`` -- an ordinary status update, no new API.
      The JobServer stops the run the same way, then sets ``status`` to
      ``killed``. A job killed before it was ever started is finalized as
      ``killed`` directly.
    * Both checks are also applied during startup reattachment, so a kill
      requested just before a JobServer restart is not lost or, worse,
      mistaken for a merely-lost job and resubmitted.

2026.8.6 -- Add SLURM submission mode (Option 1: whole-flowchart sbatch)
    * A JobServer instance can now submit each job as a single SLURM batch job
      instead of a local subprocess, controlled by a new, opt-in
      ``<root>/<jobserver-name>.ini`` config file (absent means unchanged,
      local-subprocess behavior). Supports both running the SLURM CLI directly
      on the JobServer's own host and reaching a remote cluster over SSH.
    * Added a configurable cap on how many jobs a JobServer instance keeps
      outstanding in SLURM at once.
    * If a tracked job's SLURM state goes missing or terminal while its
      datastore row still shows ``running`` (a node failure, an OOM kill, a
      cancellation, or the JobServer itself having restarted), the JobServer
      now trusts the job's own recorded outcome when available, otherwise
      safely resubmits it (flowcharts checkpoint and resume, so this does not
      redo completed work) up to a configurable retry cap before giving up.
    * A job can now request different SLURM resources (cores, memory,
      walltime, ...) than the JobServer instance's own defaults, via
      ``parameters["slurm"]`` on the job row -- gated by a new, optional
      ``[<section>.limits]`` ini section that says which fields a job may
      override and within what bounds (enumerated choices or numeric/size/
      time ranges). Secure by default: no ``.limits`` section means nothing
      is overridable. Always re-validated server-side regardless of what
      submitted the job.
    * See the User Guide for the config file format and behavior.

2025.11.22 -- Bugfix: Catching errors when starting a job.
    * Now catch any error that occurs starting a job, and set its status to 'startup
      error'. This prevents the JobServer getting into a loop repeatedly trying to
      submit the job.

2025.11.12 -- Added SEAMM_JOBSERVER and SEAMM_JOB_ID environment variables
    This PR adds support for passing job-specific metadata to spawned processes through
    environment variables. Jobs can now access their unique job ID and the name of the
    JobServer that spawned them.

    * Added SEAMM_JOB_ID and SEAMM_JOBSERVER environment variables for spawned job
      processes 
    * Added --name command-line argument to specify JobServer name (defaults to
      hostname)
    * Cleaned up docstring formatting

2024.4.12 -- Fixed issue with status of finished jobs
   * Fixed a problem if a job returned a status of None, which was reported as an
     error.

2024.4.11 -- Correcting description of this package

2024.4.5 -- Adding support for debugging
   * Use the value of the environment variable SEAMM_LOG_LEVEL to set the log level for
     jobs. DEBUG, INFO, WARNING are three useful levels.
     
2024.1.17 -- Changes to support running in Docker containers.

2023.12.12 -- Improved the output in the GUI.
   * Improved the output to the GUI
   * Fixed a bug in the file path for the status file.

2023.3.23 -- Substantial improvements to JobServer
   * Switched to independent process for Jobs, which means they are fully independent of
     the JobServer and continue to run if the JobServer stops
   * Discover existing running jobs on startup and monitor them.
   * Added status information for the machine the JobServer is on as well as Jobs
   * Provide a GUI if run from the commandline, showing the log and status.

0.9.1 (2020-05-29)
------------------

* First release on PyPI.
