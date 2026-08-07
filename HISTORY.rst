=======
History
=======
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
