=======
History
=======
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
