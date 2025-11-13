=======
History
=======
2025.11.12 -- Added SEAMM_JOBSERVER and SEAMM_JOB_ID environment variables
   * Added SEAMM_JOBSERVER and SEAMM_JOB_ID environment variables so that jobs can
     access the name of the jobserver and the job id for tagging data, etc.

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
