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

import copy
import threading
import time
from typing import TYPE_CHECKING
from typing import NamedTuple

from apache_beam.runners.worker.task_worker.handlers import TaskWorkerHandler

# This module will be imported by the task worker handler plugin system
# regardless of whether the kubernetes API is installed. It must be safe to
# import whether it will be used or not.
try:
  import kubernetes.client as client
  import kubernetes.config as config
  from kubernetes.client.rest import ApiException
except ImportError:
  client = None
  config = None
  ApiException = None

if TYPE_CHECKING:
  from typing import List

__all__ = [
    'KubeTaskWorkerHandler',
]


class KubePayload(object):
  """
  Object for holding attributes for a kubernetes job.
  """

  def __init__(self, job, namespace='default'):
    # type: (client.V1Job, str) -> None
    self.job = job
    self.namespace = namespace

  @property
  def job_name(self):
    return self.job.metadata.name

  @job_name.setter
  def job_name(self, value):
    self.job.metadata.name = value
    self.job.spec.template.metadata.labels['app'] = value
    for container in self.job.spec.template.spec.containers:
      container.name = value


class KubeJobManager(object):
  """
  Monitors jobs submitted by the KubeTaskWorkerHandler. Responsible for
  notifying `watch`ed handlers if their Kubernetes job is deleted.
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
        if handler.alive and not handler.job_exists():
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
  The Kubernetes task handler.
  """

  _lock = threading.Lock()
  _monitor = None  # type: KubeJobManager

  api = None  # type: client.BatchV1Api

  @property
  def monitor(self):
    # type: () -> KubeJobManager
    if KubeTaskWorkerHandler._monitor is None:
      with KubeJobManager._lock:
        KubeTaskWorkerHandler._monitor = KubeJobManager()
    return KubeTaskWorkerHandler._monitor

  def job_exists(self):
    # type: () -> bool
    """
    Return whether or not the Kubernetes job exists.
    """
    try:
      self.api.read_namespaced_job_status(
          self.task_payload.job.metadata.name, self.task_payload.namespace)
    except ApiException:
      return False
    return True

  def submit_job(self, payload):
    # type: (KubePayload) -> client.V1Job
    """
    Submit a Kubernetes job.
    """
    # Patch some handler specific env variables into the job
    job = copy.deepcopy(payload.job)  # type: client.V1Job

    env = [
      client.V1EnvVar(name='TASK_WORKER_ID', value=self.worker_id),
      client.V1EnvVar(name='TASK_WORKER_CONTROL_ADDRESS',
                      value=self.control_address),
    ]
    if self.credentials:
      env.extend([
        client.V1EnvVar(name='TASK_WORKER_CREDENTIALS',
                        value=self.credentials),
      ])

    for container in job.spec.template.spec.containers:
      if container.env is None:
        container.env = []
      container.env.extend(env)

    return self.api.create_namespaced_job(
        body=job,
        namespace=payload.namespace)

  def delete_job(self):
    # type: () -> client.V1Status
    """
    Delete the kubernetes job.
    """
    return self.api.delete_namespaced_job(
        name=self.task_payload.job.metadata.name,
        namespace=self.task_payload.namespace,
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))

  def start_remote(self):
    # type: () -> None
    with KubeTaskWorkerHandler._lock:
      config.load_kube_config()
      self.api = client.BatchV1Api()

    self.submit_job(self.task_payload)
    self.monitor.watch(self)
