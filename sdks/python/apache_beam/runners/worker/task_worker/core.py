#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
TaskWorker core user facing PTransforms that allows packaging elements as
TaskableValue for beam task workers.
"""

import copy
from typing import TYPE_CHECKING

import apache_beam as beam
from apache_beam.runners.worker.task_worker.handlers import TaskableValue

if TYPE_CHECKING:
  from typing import Any, Optional, Callable, Iterator
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.transforms.environments import Environment


TASK_WORKER_ENV_TYPES = {'Docker', 'Process'}
TASK_WORKER_SDK_ENTRYPOINT = 'apache_beam.runners.worker.task_worker.task_worker_main'


class WrapFn(beam.DoFn):
  """
  Wraps the given element into a TaskableValue if there's non-empty task
  payload. User can pass in wrapper callable to modify payload per element.
  """

  def process(
      self,
      element,  # type: Any
      urn='local',  # type: str
      wrapper=None,  # type: Optional[Callable[[Any, Any], Any]]
      env=None,  # type: Optional[Environment]
      payload=None  # type: Optional[Any]
  ):
    # type: (...) -> Iterator[Any]
    """
    Args:
      element: any ptransform element
      urn: id of the task worker handler
      wrapper: optional callable which can be used to modify a payload
          per-element.
      env : Environment for the task to be run in
      payload : Payload containing settings for the task worker handler
    """
    # override payload if given a wrapper function, which will vary per
    # element
    if wrapper:
      payload = wrapper(element, copy.deepcopy(payload))

    if payload:
      result = TaskableValue(element, urn, env=env, payload=payload)
    else:
      result = element
    yield result


class UnWrapFn(beam.DoFn):
  """
  Unwraps the TaskableValue into its original value, so that when constructing
  transforms user doesn't need to worry about the element type if it is
  taskable or not.
  """

  def process(self, element):

    if isinstance(element, TaskableValue):
      yield element.value
    else:
      yield element


class BeamTask(beam.PTransform):
  """
  Utility transform that wraps a group of transforms, and makes it a Beam
  "Task" that can be delegated to a task worker to run remotely.

  The main structure is like this:

  ( pipe
    | Wrap
    | Reshuffle
    | UnWrap
    | User Transform1
    | ...
    | User TransformN
    | Reshuffle
  )

  The use of reshuffle is to make sure stage fusing doesn't try to fuse the
  section we want to run with the inputs of this xform; reason being we need
  the start of a stage to get data inputs that are *TaskableValue*, so that
  the bundle processor will recognize that and will engage Task Workers.

  We end with a Reshuffle for similar reason, so that the next section of the
  pipeline doesn't gets fused with the transforms provided, which would end up
  being executed remotely in a remote task worker.

  By default, we use the local task worker, but subclass could specify the
  type of task worker to use by specifying the ``urn``, and override the
  ``get_payload`` method to return meaningful payloads to that type of task
  worker.
  """

  # the urn for the registered task worker handler, default to use local task
  # worker
  urn = 'local'  # type: str

  # the sdk harness entry point
  SDK_HARNESS_ENTRY_POINT = TASK_WORKER_SDK_ENTRYPOINT

  def __init__(self, transform, wrapper=None, env=None):
    # type: (beam.PTransform, Optional[Callable[[Any, Any], Any]], Optional[beam.transforms.environments.Environment]) -> None
    self._wrapper = wrapper
    self._env = env
    self._transform = transform

  def get_payload(self, options):
    # type: (PipelineOptions) -> Optional[Any]
    """
    Subclass should implement this to generate payload for TaskableValue.
    Default to None.
    """
    return None

  @staticmethod
  def _has_tagged_outputs(xform):
    # type: (beam.PTransform) -> bool
    """Checks to see if we have tagged output for the given PTransform."""
    if isinstance(xform, beam.core._MultiParDo):
      return True
    elif isinstance(xform, beam.ptransform._ChainedPTransform) \
        and isinstance(xform._parts[-1], beam.core._MultiParDo):
      return True
    return False

  def expand(self, pcoll):
    # type: (beam.pvalue.PCollection) -> beam.pvalue.PCollection
    payload = self.get_payload(pcoll.pipeline.options)
    result = (
      pcoll
      | 'Wrap' >> beam.ParDo(WrapFn(), urn=self.urn, wrapper=self._wrapper,
                             env=self._env, payload=payload)
      | 'StartStage' >> beam.Reshuffle()
      | 'UnWrap' >> beam.ParDo(UnWrapFn())
      | self._transform
    )
    if self._has_tagged_outputs(self._transform):
      # for xforms that ended up with tagged outputs, we don't want to
      # add reshuffle, because it will be a stage split point already,
      # also adding reshuffle would error since we now have a tuple of
      # pcollections.
      return result
    return result | 'EndStage' >> beam.Reshuffle()
