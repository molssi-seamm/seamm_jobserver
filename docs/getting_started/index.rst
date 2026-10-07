***************
Getting Started
***************

The JobServer is the service that runs the jobs you submit from the SEAMM GUI
or the web interface. ``seamm-manager install`` installs it and
``seamm-manager services start`` runs it as a service of the machine; you
never start it by hand. ``seamm-manager services status`` shows it running
beside the web interface::

    seamm-manager services status

Out of the box it runs each job as a subprocess on its own machine, as many at
once as it is allowed. One file turns it into a dispatcher that can run jobs
under the machine's own queueing system or send them to a cluster over ssh:
``<root>/<name>.ini``, where ``<root>`` is the installation (``~/SEAMM`` by
default) and ``<name>`` the JobServer's name, which defaults to the hostname.
A minimal file that sends jobs to a SLURM cluster reached over passwordless
ssh, with the job directories staged there and back::

    [DEFAULT]
    default = cluster

    [cluster]
    transport = ssh
    host = cluster
    export = NONE
    remote_root = /scratch/me/seamm_jobs
    remote_run_from_jobserver = /path/to/SEAMM/venv/bin/run_from_jobserver
    account = myaccount
    partition = normal
    ntasks = 4
    mem_per_cpu = 2G
    time = 04:00:00
    max_concurrent_jobs = 20

    [cluster.limits]
    overridable = ntasks, mem_per_cpu, time
    ntasks.max = 64
    time.max = 2-00:00:00

The JobServer reads the file when it starts, so restart it after editing::

    seamm-manager services restart

The :ref:`User Guide <user-guide>` describes every key, several queues in one
file, PBS, what happens to a job on a remote cluster, per-job resource
overrides and where a job's calculations run. The main SEAMM documentation
has a step-by-step how-to, "Configure the JobServer's Queues", with real
examples.
