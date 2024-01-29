import logging
import os
import pwd
import subprocess
from subprocess import CalledProcessError

from dask_gateway import GatewayCluster

# Basic logging configuration
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger("htcdaskgateway.GatewayCluster")

class HTCGatewayCluster(GatewayCluster):
    def __init__(self, scheduler_proxy_ip='131.225.219.43', **kwargs):
        self.scheduler_proxy_ip = scheduler_proxy_ip
        self.batchWorkerJobs = []
        super().__init__(**kwargs)

    async def _stop_async(self):
        self.destroy_all_batch_clusters()
        await super()._stop_async()
        self.status = "closed"

    def scale_batch_workers(self, n):
        nb_group = os.environ.get('NB_GROUP', 'default_value')

        if nb_group == 'dune':
            # Perform the actions specific to 'dune'
            self.perform_dune_specific_actions(n)
        elif nb_group == 'cms':
            # Perform the actions specific to 'cms'
            self.perform_cms_specific_actions(n)
        else:
            # Perform default actions or handle the unexpected value
            self.handle_default_or_unexpected_group(n)


    def scale(self, n, worker_type='htcondor', **kwargs):
        nb_group = os.environ.get('NB_GROUP', 'default_value')
        logger.warning(f"worker_type: {worker_type}")
        if nb_group == 'dune':
            if 'condor' in worker_type:
                self.batchWorkerJobs.clear()
                logger.info(f"Scaling: {n} HTCondor workers")
                try:
                    self.batchWorkerJobs.append(self.scale_batch_workers(n))
                    logger.debug("New Cluster state")
                    logger.debug(self.batchWorkerJobs)
                    return self.gateway.scale_cluster(self.name, n, **kwargs)
                except Exception as e:
                    logger.error(f"A problem occurred while scaling via HTCondor: {e}")
                    return False

    def scale_batch_workers(self, n, path_to_executable):
        username = pwd.getpwuid(os.getuid())[0]
        security = self.security
        cluster_name = self.name
        tmproot = f"/uscmst1b_scratch/lpc1/3DayLifetime/{username}/{cluster_name}"
        condor_logdir, credentials_dir, worker_space_dir = self.create_directories(tmproot)

        self.write_credentials(credentials_dir, security)
        self.prepare_submission_files(n, tmproot, condor_logdir, credentials_dir, worker_space_dir)

        logger.info(f"Sandbox: {tmproot}")
        logger.debug(f"Submitting HTCondor job(s) for {n} workers")
        return self.jobsub_submit_job(tmproot)

    def create_directories(self, tmproot):
        condor_logdir = f"{tmproot}/condor"
        credentials_dir = f"{tmproot}/dask-credentials"
        worker_space_dir = f"{tmproot}/dask-worker-space"
        for dir_path in [tmproot, condor_logdir, credentials_dir, worker_space_dir]:
            os.makedirs(dir_path, exist_ok=True)
        return condor_logdir, credentials_dir, worker_space_dir

    def write_credentials(self, credentials_dir, security):
        with open(f"{credentials_dir}/dask.crt", 'w') as f:
            f.write(security.tls_cert)
        with open(f"{credentials_dir}/dask.pem", 'w') as f:
            f.write(security.tls_key)
        with open(f"{credentials_dir}/api-token", 'w') as f:
            f.write(os.environ['JUPYTERHUB_API_TOKEN'])

    def prepare_submission_files(self, n, tmproot, condor_logdir, credentials_dir, worker_space_dir):
        image_name = "/cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest"

        # Prepare JDL
        jdl = f"""executable = start.sh
arguments = {cluster_name} htcdask-worker_$(Cluster)_$(Process)
output = condor/htcdask-worker$(Cluster)_$(Process).out
error = condor/htcdask-worker$(Cluster)_$(Process).err
log = condor/htcdask-worker$(Cluster)_$(Process).log
request_cpus = 4
request_memory = 2100
should_transfer_files = yes
transfer_input_files = {credentials_dir}, {worker_space_dir}, {condor_logdir}
Queue {n}
"""

        with open(f"{tmproot}/htcdask_submitfile.jdl", 'w+') as f:
            f.writelines(jdl)

        # Prepare singularity command
        singularity_cmd = f"""#!/bin/bash
export APPTAINERENV_DASK_GATEWAY_WORKER_NAME=$2
export APPTAINERENV_DASK_GATEWAY_API_URL="https://dask-gateway-api.fnal.gov/api"
export APPTAINERENV_DASK_GATEWAY_CLUSTER_NAME=$1
export APPTAINERENV_DASK_GATEWAY_API_TOKEN=/etc/dask-credentials/api-token
export APPTAINERENV_DASK_DISTRIBUTED__LOGGING__DISTRIBUTED="debug"

worker_space_dir=${{PWD}}/dask-worker-space/$2
mkdir $worker_space_dir

/cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer exec -B ${{worker_space_dir}}:/srv/dask-worker-space -B dask-credentials:/etc/dask-credentials {image_name} \
dask-worker --name $2 --tls-ca-file /etc/dask-credentials/dask.crt --tls-cert /etc/dask-credentials/dask.crt --tls-key /etc/dask-credentials/dask.pem --worker-port 10000:10070 --no-nanny --no-dashboard --local-directory /srv --scheduler-sni daskgateway-{cluster_name} --nthreads 1 tls://{self.scheduler_proxy_ip}:80"""

        with open(f"{tmproot}/start.sh", 'w+') as f:
            f.writelines(singularity_cmd)
        os.chmod(f"{tmproot}/start.sh", 0o775)

    def jobsub_submit_job(self, tmproot, n, executable_file_path):
        os.environ['LS_COLORS'] = "ExGxBxDxCxEgEdxbxgxcxd"  # Avoiding a bug in Farruk's condor_submit wrapper
        cmd = "/opt/jobsub_lite/bin/jobsub_submit -N {n} -G dune --global-pool dune file://{executable_file_path}| grep -oP '(?<=cluster )[^ ]*'"
        try:
            call = subprocess.check_output(['sh', '-c', cmd], cwd=tmproot)
            clusterid = call.decode().rstrip()[:-1]
            worker_dict = {'ClusterId': clusterid, 'Iwd': tmproot}

            cmd = f"/opt/jobsub_lite/bin/jobsub_q -G dune --global-pool dune --jobid {clusterid}"
            call = subprocess.check_output(['sh', '-c', cmd], cwd=tmproot)
            scheddname = call.decode().rstrip()
            worker_dict['ScheddName'] = scheddname

            logger.info(f"Success! Submitted HTCondor jobs to {scheddname} with ClusterId {clusterid}")
            return worker_dict
        except CalledProcessError as e:
            logger.error("Error submitting HTCondor jobs: " + str(e))
            return None

    def destroy_all_batch_clusters(self):
        logger.info("Shutting down HTCondor worker jobs (if any)")
        if not self.batchWorkerJobs:
            return
