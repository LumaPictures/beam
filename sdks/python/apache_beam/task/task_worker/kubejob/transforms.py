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
Transforms for kubernetes task workers.
"""

from __future__ import absolute_import

import re
from typing import TYPE_CHECKING

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.environments import DockerEnvironment
from apache_beam.runners.worker.task_worker.core import BeamTask

try:
  import kubernetes.client
except ImportError:
  raise ImportError(
    'Kubernetes task worker is not supported by this environment '
    '(could not import kubernetes API).')

if TYPE_CHECKING:
  from apache_beam.task.task_worker.kubejob.handler import KubePayload

__all__ = [
  'KubeJobOptions',
  'KubeTask',
]


class KubeJobOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--namespace',
        type=str,
        default='default',
        help='The namespace to submit kubernetes task worker jobs to.')


class KubeTask(BeamTask):
  """
  Kubernetes task worker transform.
  """

  urn = 'k8s'

  K8S_JOB_NAME_RE = re.compile(r'[a-z][a-zA-Z0-9-.]*')

  def get_payload(self, options):
    # type: (PipelineOptions) -> KubePayload
    """
    Get a task payload for configuring a kubernetes job.
    """
    from apache_beam.task.task_worker.kubejob.handler import KubePayload

    name = self.label
    name = name[0].lower() + name[1:]
    if not self.K8S_JOB_NAME_RE.match(name):
      raise ValueError(
          'Kubernetes job name must start with a lowercase letter and use '
          'only - or . special characters')

    image = DockerEnvironment.get_container_image_from_options(options)

    container = kubernetes.client.V1Container(
        name=name,
        image=image,
        command=['python', '-m', self.SDK_HARNESS_ENTRY_POINT],
        env=[])

    template = kubernetes.client.V1PodTemplateSpec(
        metadata=kubernetes.client.V1ObjectMeta(
            labels={'app': name}),
        spec=kubernetes.client.V1PodSpec(
            restart_policy='Never',
            containers=[container]))

    spec = kubernetes.client.V1JobSpec(
        template=template,
        backoff_limit=4)

    job = kubernetes.client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata=kubernetes.client.V1ObjectMeta(name=name),
        spec=spec)

    return KubePayload(
        job,
        namespace=options.view_as(KubeJobOptions).namespace)
