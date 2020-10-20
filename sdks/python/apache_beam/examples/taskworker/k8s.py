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

import argparse
import logging
from typing import TYPE_CHECKING

import apache_beam as beam
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.task.task_worker.k8s.transforms import KubeTask

if TYPE_CHECKING:
  from typing import Any
  from apache_beam.task.task_worker.k8s.handler import KubeTaskProperties


def process(element):
  # type: (int) -> int
  import time
  start = time.time()
  print('processing...')
  # represents an expensive process of some sort
  time.sleep(element)
  print('processing took {:0.6f} s'.format(time.time() - start))
  return element


def _per_element_wrapper(element, payload):
  # type: (Any, KubeTaskProperties) -> KubeTaskProperties
  """
  Callback to modify the kubernetes job properties per-element.
  """
  import copy
  result = copy.copy(payload)
  result.name += '-{}'.format(element)
  return result


def run(argv=None, save_main_session=True):
  """
  Run a pipeline that submits two kubernetes jobs, each that simply sleep
  """
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(DirectOptions).direct_running_mode = 'multi_processing'

  with beam.Pipeline(options=pipeline_options) as pipe:
    (
      pipe
      | beam.Create([20, 42])
      | 'kubetask' >> KubeTask(
        beam.Map(process),
        wrapper=_per_element_wrapper)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
