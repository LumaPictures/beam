#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Kubernetes task worker implementation.
"""

from __future__ import absolute_import

import threading
import time
from typing import TYPE_CHECKING

from apache_beam.runners.worker.task_worker.handlers import TaskWorkerHandler

try:
  from kubernetes.client import BatchV1Api
  from kubernetes.client import V1EnvVar
  from kubernetes.client import V1Container
  from kubernetes.client import V1PodTemplateSpec
  from kubernetes.client import V1PodSpec
  from kubernetes.client import V1ObjectMeta
  from kubernetes.client import V1Job
  from kubernetes.client import V1JobSpec
  from kubernetes.client import V1DeleteOptions
  from kubernetes.client.rest import ApiException
  from kubernetes.config import load_kube_config
except ImportError:
  BatchV1Api = None
  V1EnvVar = None
  V1Container = None
  V1PodTemplateSpec = None
  V1PodSpec = None
  V1ObjectMeta = None
  V1Job = None
  V1JobSpec = None
  V1DeleteOptions = None
  ApiException = None
  load_kube_config = None

if TYPE_CHECKING:
  from typing import List

__all__ = [
    'KubeTaskProperties',
    'KubeTaskWorkerHandler',
]


class KubeTaskProperties(object):
  """
  Object for describing kubernetes job properties for a task worker.
  """

  def __init__(
      self,
      name,  # type: str
      namespace='default',
      # FIXME: Use PortableOptions.environment_config?
      container='apache/beam_python3.8_sdk:2.26.0.dev',
      command=('python', '-m',
               'apache_beam.runners.worker.task_worker.task_worker_main')
  ):
    # type: (...) -> None
    self.name = name
    self.namespace = namespace
    self.container = container
    self.command = command


class KubeJobManager(object):
  """
  Monitors jobs submitted by the KubeTaskWorkerHandler.
  """

  _thread = None  # type: threading.Thread
  _lock = threading.Lock()

  def __init__(self):
    self._handlers = []  # type: List[KubeTaskWorkerHandler]

  def is_started(self):
    """
    Return if the manager is currently running or not.
    """
    return self._thread is not None

  def _start(self):
    self._thread = threading.Thread(target=self._run)
    self._thread.daemon = True
    self._thread.start()

  def _run(self, interval=5.0):
    while True:
      for handler in self._handlers:
        # If the handler thinks it's alive but it's not actually, change its
        # alive state.
        if handler.alive and not handler.is_alive():
          handler.alive = False
      time.sleep(interval)

  def watch(self, handler):
    # type: (KubeTaskWorkerHandler) -> None
    """
    Monitor the passed handler checking periodically that the job is still
    running.
    """
    if not self.is_started():
      self._start()
    self._handlers.append(handler)


@TaskWorkerHandler.register_urn('k8s')
class KubeTaskWorkerHandler(TaskWorkerHandler):
  """
  The kubernetes task handler.
  """

  _lock = threading.Lock()
  _monitor = None  # type: KubeJobManager

  api = None  # type: BatchV1Api

  @property
  def monitor(self):
    # type: () -> KubeJobManager
    if KubeTaskWorkerHandler._monitor is None:
      with KubeJobManager._lock:
        KubeTaskWorkerHandler._monitor = KubeJobManager()
    return KubeTaskWorkerHandler._monitor

  def is_alive(self):
    try:
      self.api.read_namespaced_job_status(
        self.task_payload.name, self.task_payload.namespace)
    except ApiException:
      return False
    return True

  def create_job(self):
    # type: () -> V1Job
    """
    Create a kubernetes job object.
    """

    env = [
      V1EnvVar(name='TASK_WORKER_ID', value=self.worker_id),
      V1EnvVar(name='TASK_WORKER_CONTROL_ADDRESS', value=self.control_address),
    ]
    if self.credentials:
      env.extend([
        V1EnvVar(name='TASK_WORKER_CREDENTIALS', value=self.credentials),
      ])

    # Configure Pod template container
    container = V1Container(
      name=self.task_payload.name,
      image=self.task_payload.container,
      command=self.task_payload.command,
      env=env)
    # Create and configure a spec section
    template = V1PodTemplateSpec(
      metadata=V1ObjectMeta(
        labels={'app': self.task_payload.name}),
      spec=V1PodSpec(restart_policy='Never', containers=[container]))
    # Create the specification of deployment
    spec = V1JobSpec(
      template=template,
      backoff_limit=4)
    # Instantiate the job object
    job = V1Job(
      api_version='batch/v1',
      kind='Job',
      metadata=V1ObjectMeta(name=self.task_payload.name),
      spec=spec)

    return job

  def submit_job(self, job):
    # type: (V1Job) -> str
    """
    Submit a kubernetes job.
    """
    api_response = self.api.create_namespaced_job(
      body=job,
      namespace=self.task_payload.namespace)
    return api_response.metadata.uid

  def delete_job(self):
    """
    Delete the kubernetes job.
    """
    return self.api.delete_namespaced_job(
      name=self.task_payload.name,
      namespace=self.task_payload.namespace,
      body=V1DeleteOptions(
        propagation_policy='Foreground',
        grace_period_seconds=5))

  def start_remote(self):
    # type: () -> None
    with KubeTaskWorkerHandler._lock:
      load_kube_config()
      self.api = BatchV1Api()
    job = self.create_job()
    self.submit_job(job)
    self.monitor.watch(self)
