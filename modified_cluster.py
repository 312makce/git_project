
import asyncio
import logging
import os
import socket
import sys
import pwd
import tempfile
import subprocess
import weakref
import pprint

from distributed.core import Status
from dask_gateway import GatewayCluster

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger("htcdaskgateway.GatewayCluster")

class HTCGatewayCluster(GatewayCluster):
    
    def __init__(self, **kwargs):
        self.scheduler_proxy_ip = kwargs.pop('', '131.225.219.43')
        self.batchWorkerJobs = []
        super().__init__(**kwargs)
   
    async def _stop_async(self):
        self.destroy_all_batch_clusters()
        await super()._stop_async()
        self.status = "closed"
    
    def scale(self, n, executable_file_path, **kwargs):
        logger.info("Scaling to "+str(n)+" workers using jobsub_submit")
        cmd_template = "jobsub_submit -G dune --global-pool duneglobal -N {} file://{}"
        cmd = cmd_template.format(n, executable_file_path)
        try:
            result = subprocess.check_output(['sh', '-c', cmd], cwd=tempfile.mkdtemp())
            logger.info("Job submission successful. Result: {}".format(result.decode('utf-8')))
        except subprocess.CalledProcessError as e:
            logger.error("Error submitting HTCondor jobs: {}".format(e.output.decode('utf-8')))
