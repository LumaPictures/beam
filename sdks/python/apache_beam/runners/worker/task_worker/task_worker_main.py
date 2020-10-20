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
"""TaskWorker entry point."""

from __future__ import absolute_import

import os
import logging
import sys
import traceback

from apache_beam.runners.worker.sdk_worker_main import _load_main_session
from apache_beam.runners.worker.task_worker.handlers import BundleProcessorTaskWorker
from apache_beam.runners.worker.task_worker.handlers import TaskWorkerHandler

# This module is experimental. No backwards-compatibility guarantees.


def main(unused_argv):
  """Main entry point for Starting up TaskWorker."""

  # TODO: Do we want to set up the logging service here?

  # below section is the same as in sdk_worker_main, for loading pickled main
  # session if there's any
  if 'SEMI_PERSISTENT_DIRECTORY' in os.environ:
    semi_persistent_directory = os.environ['SEMI_PERSISTENT_DIRECTORY']
  else:
    semi_persistent_directory = None

  logging.info('semi_persistent_directory: %s', semi_persistent_directory)

  try:
    _load_main_session(semi_persistent_directory)
  except Exception:  # pylint: disable=broad-except
    exception_details = traceback.format_exc()
    logging.error(
        'Could not load main session: %s', exception_details, exc_info=True)

  worker_id = os.environ['TASK_WORKER_ID']
  control_address = os.environ['TASK_WORKER_CONTROL_ADDRESS']
  if 'TASK_WORKER_CREDENTIALS' in os.environ:
    credentials = os.environ['TASK_WORKER_CREDENTIALS']
  else:
    credentials = None

  TaskWorkerHandler.load_plugins()

  # exception should be handled already by task workers
  BundleProcessorTaskWorker.execute(worker_id, control_address,
                                    credentials=credentials)


if __name__ == '__main__':
  main(sys.argv)
