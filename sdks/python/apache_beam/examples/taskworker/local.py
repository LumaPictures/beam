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
Basic pipeline to test using TaskWorkers.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.runners.worker.task_worker.core import BeamTask


class TestFn(beam.DoFn):

  def process(self, element, side):
    from apache_beam.runners.worker.task_worker.handlers import TaskableValue

    for s in side:
      if isinstance(element, TaskableValue):
        value = element.value
      else:
        value = element
      print(value + s)
      yield value + s


def run(argv=None, save_main_session=True):
  """
  Run a pipeline that executes each element using the local task worker.
  """
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(DirectOptions).direct_running_mode = 'multi_processing'

  with beam.Pipeline(options=pipeline_options) as pipe:

    A = (
      pipe
      | 'A' >> beam.Create(range(3))
    )

    B = (
      pipe
      | beam.Create(range(2))
      | BeamTask(beam.ParDo(TestFn(), beam.pvalue.AsList(A)),
                 wrapper=lambda x, _: x)
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
