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

from apache_beam.runners.worker.task_worker.core import BeamTask


class KubeTask(BeamTask):
  """
  Kubernetes task worker transform.
  """

  urn = 'k8s'

  K8S_NAME_RE = re.compile(r'[a-z][a-zA-Z0-9-.]*')

  def get_payload(self):
    """
    Get a task payload for configuring a kubernetes job.
    """
    from apache_beam.task.task_worker.k8s.handler import KubeTaskProperties

    name = self.label
    name = name[0].lower() + name[1:]
    # NOTE: k8s jobs have very restricted names
    assert self.K8S_NAME_RE.match(name), 'Kubernetes job name must start ' \
                                         'with a lowercase letter and use ' \
                                         'only - or . special characters'
    # FIXME: How to make this name auto-unique? Default wrapper?
    return KubeTaskProperties(name=name)
