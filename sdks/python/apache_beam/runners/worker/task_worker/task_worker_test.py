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
"""Tests for apache_beam.runners.worker.task_worker."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import mock
import threading
import unittest
from builtins import range
from collections import defaultdict

import grpc
from future.utils import raise_

from apache_beam.coders import coders, coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import beam_task_worker_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.worker.task_worker import handlers
from apache_beam.runners.worker.data_plane import GrpcClientDataChannelFactory
from apache_beam.runners.worker.sdk_worker import CachingStateHandler
from apache_beam.transforms import window


# -- utilities for testing, mocking up test objects
class _MockBundleProcessorTaskWorker(handlers.BundleProcessorTaskWorker):
  """
  A mocked version of BundleProcessorTaskWorker, responsible for recording the
  requests it received, and provide response to each request type by a passed
  in dictionary as user desired.
  """
  def __init__(self, worker_id, server_url, credentials=None,
               requestRecorder=None, responsesByRequestType=None):
    super(_MockBundleProcessorTaskWorker, self).__init__(
      worker_id,
      server_url,
      credentials=credentials
    )
    self.requestRecorder = requestRecorder or []
    self.responsesByRequestType = responsesByRequestType or {}

  def do_instruction(self, request):
    # type: (beam_task_worker_pb2.TaskInstructionRequest) -> beam_task_worker_pb2.TaskInstructionResponse
    request_type = request.WhichOneof('request')
    self.requestRecorder.append(request)
    return self.responsesByRequestType.get(request_type)


@handlers.TaskWorkerHandler.register_urn('unittest')
class _MockTaskWorkerHandler(handlers.TaskWorkerHandler):
  """
  Register a mocked version of task handler only used for "unittest"; will start
  a ``_MockBundleProcessorTaskWorker`` for each discovered TaskableValue.

  Main difference is that it returns the started task worker object, for easier
  testing.
  """

  def __init__(self,
               state,  # type: CachingStateHandler
               provision_info,
               # type: Union[beam_provision_api_pb2.ProvisionInfo, ExtendedProvisionInfo]
               grpc_server,  # type: TaskGrpcServer
               environment,  # type: Environment
               task_payload,  # type: Any
               credentials=None,  # type: Optional[str]
               worker_id=None,  # type: Optional[str]
               responseByRequestType=None
               ):
    super(_MockTaskWorkerHandler, self).__init__(state, provision_info,
                                                 grpc_server, environment,
                                                 task_payload,
                                                 credentials=credentials,
                                                 worker_id=worker_id)
    self.responseByRequestType = responseByRequestType

  def start_remote(self):
    # type: () -> _MockBundleProcessorTaskWorker
    """starts a task worker local to the task worker handler."""
    obj = _MockBundleProcessorTaskWorker(
      self.worker_id,
      self.control_address,
      self.credentials,
      responsesByRequestType=self.responseByRequestType)
    run_thread = threading.Thread(target=obj.run)
    run_thread.daemon = True
    run_thread.start()

    return obj

  def start_worker(self):
    # type: () -> _MockBundleProcessorTaskWorker
    return self.start_remote()


class _MockTaskGrpcServer(handlers.TaskGrpcServer):
  """
  Mocked version of TaskGrpcServer, using mocked version of data channel factory
  and cache handler.
  """

  def __init__(self, instruction_id, max_workers=1, data_store=None):
    dummy_state_handler = _MockCachingStateHandler(None, None)
    dummy_data_channel_factory = GrpcClientDataChannelFactory()

    super(_MockTaskGrpcServer, self).__init__(dummy_state_handler, max_workers,
                                              data_store or {},
                                              dummy_data_channel_factory,
                                              instruction_id)


class _MockCachingStateHandler(CachingStateHandler):
  """
  Mocked CachingStateHandler, mainly for patching the thread local variable
  ``_context`` and create the ``cache_token`` attribute on it.
  """

  def __init__(self, underlying_state, global_state_cache):
    self._underlying = underlying_state
    self._state_cache = global_state_cache
    self._context = threading.local()

    self._context.cache_token = ''


class _MockDataInputOperation(object):
  """
  A mocked version of DataInputOperation, responsible for recording and decoding
  data for testing.
  """

  def __init__(self, coder):
    self.coder = coder
    self.decoded = []
    self.splitting_lock = threading.Lock()
    self.windowed_coder_impl = self.coder.get_impl()

    with self.splitting_lock:
      self.index = -1
      self.stop = float('inf')

  def output(self, decoded_value):
    self.decoded.append(decoded_value)


def prep_responses_by_request_type(worker_id, delayed_applications=(),
                                   require_finalization=False, process_error=None,
                                   shutdown_error=None):
  return {
    'create': beam_task_worker_pb2.TaskInstructionResponse(
      create=beam_task_worker_pb2.CreateResponse(),
      instruction_id=worker_id
    ),
    'process_bundle': beam_task_worker_pb2.TaskInstructionResponse(
      process_bundle=beam_task_worker_pb2.ProcessorProcessBundleResponse(
        delayed_applications=list(delayed_applications),
        require_finalization=require_finalization),
      instruction_id=worker_id,
      error=process_error
    ),
    'shutdown': beam_task_worker_pb2.TaskInstructionResponse(
        shutdown=beam_task_worker_pb2.ShutdownResponse(),
        instruction_id=worker_id,
        error=shutdown_error
      )
  }


def prep_bundle_processor_descriptor(bundle_id):
  return beam_fn_api_pb2.ProcessBundleDescriptor(
    id='test_bundle_{}'.format(bundle_id),
    transforms={
      str(bundle_id): beam_runner_api_pb2.PTransform(unique_name=str(bundle_id))
    })


class TaskWorkerHandlerTest(unittest.TestCase):

  @staticmethod
  def _get_task_worker_handler(worker_id, resp_by_type, instruction_id,
                               max_workers=1, data_store=None):
    server = _MockTaskGrpcServer(instruction_id, max_workers=max_workers,
                                 data_store=data_store)
    return _MockTaskWorkerHandler(server.state_handler, None, server, None,
                                  None,
                                  worker_id=worker_id,
                                  responseByRequestType=resp_by_type)

  def test_execute_success(self):
    """
    Test when a TaskWorkerHandler successfully executed one life cycle.
    """
    dummy_process_bundle_descriptor = prep_bundle_processor_descriptor(1)

    worker_id = 'test_task_worker_1'
    instruction_id = 'test_instruction_1'
    resp_by_type = prep_responses_by_request_type(worker_id)

    test_handler = self._get_task_worker_handler(worker_id, resp_by_type,
                                                 instruction_id)

    proxy_data_channel_factory = task_worker.ProxyGrpcClientDataChannelFactory(
      test_handler._grpc_server.data_address
    )

    test_worker = test_handler.start_worker()

    try:
      delayed, requests = test_handler.execute(proxy_data_channel_factory,
                                               dummy_process_bundle_descriptor)
      self.assertEquals(len(delayed), 0)
      self.assertEquals(requests, False)
    finally:
      test_handler._grpc_server.close()

    # check that the requests we received are as expected
    expected = [
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=worker_id,
        create=beam_task_worker_pb2.CreateRequest(
          process_bundle_descriptor=dummy_process_bundle_descriptor,
          state_handler_endpoint=endpoints_pb2.ApiServiceDescriptor(
            url=test_handler._grpc_server.state_address),
          data_factory=beam_task_worker_pb2.GrpcClientDataChannelFactory(
            transmitter_url=proxy_data_channel_factory.transmitter_url,
            worker_id=proxy_data_channel_factory.worker_id,
            credentials=proxy_data_channel_factory._credentials
          ))),
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=worker_id,
        process_bundle=beam_task_worker_pb2.ProcessorProcessBundleRequest()),
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=worker_id,
        shutdown=beam_task_worker_pb2.ShutdownRequest())
    ]

    self.assertEquals(test_worker.requestRecorder, expected)

  def test_execute_failure(self):
    """
    Test when a TaskWorkerHandler fails to process a bundle.
    """
    dummy_process_bundle_descriptor = prep_bundle_processor_descriptor(1)

    worker_id = 'test_task_worker_1'
    instruction_id = 'test_instruction_1'
    resp_by_type = prep_responses_by_request_type(worker_id, process_error='error')

    test_handler = self._get_task_worker_handler(worker_id, resp_by_type,
                                                 instruction_id)
    proxy_data_channel_factory = task_worker.ProxyGrpcClientDataChannelFactory(
      test_handler._grpc_server.data_address
    )

    test_handler.start_worker()

    try:
      with self.assertRaises(task_worker.TaskWorkerProcessBundleError):
        print(test_handler.execute)
        test_handler.execute(
          proxy_data_channel_factory,
          dummy_process_bundle_descriptor)

      test_handler.stop_worker()
    finally:
      test_handler._grpc_server.close()


class BundleProcessorTaskHelperTest(unittest.TestCase):

  @staticmethod
  def _get_test_int_coder():
    return coders.WindowedValueCoder(coders.VarIntCoder(),
                                     coders.GlobalWindowCoder())

  @staticmethod
  def _get_test_pickle_coder():
    return coders.WindowedValueCoder(coders.FastPrimitivesCoder(),
                                     coders.GlobalWindowCoder())

  @staticmethod
  def _prep_elements(elements):
    return [window.GlobalWindows.windowed_value(elem) for elem in elements]

  @staticmethod
  def _prep_encoded_data(coder, elements, instruction_id, transform_id):
    temp_out = coder_impl.create_OutputStream()
    raw_bytes = []

    for elem in elements:
      encoded = coder.encode(elem)
      raw_bytes.append(encoded)
      coder.get_impl().encode_to_stream(elem, temp_out, True)

    data = beam_fn_api_pb2.Elements.Data(
      instruction_id=instruction_id,
      transform_id=transform_id,
      data=temp_out.get()
    )
    return raw_bytes, data

  def test_data_split_with_task_worker(self):
    """
    Test that input data is split correctly by BundleProcessorTaskHelper.
    """
    test_coder = self._get_test_pickle_coder()
    mocked_op = _MockDataInputOperation(test_coder)

    test_elems = self._prep_elements(
      [task_worker.TaskableValue(i, 'unittest') for i in range(5)])
    instruction_id = 'test_instruction_1'
    transform_id = 'test_transform_1'

    test_task_helper = task_worker.BundleProcessorTaskHelper(instruction_id)
    raw_bytes, data = self._prep_encoded_data(test_coder, test_elems,
                                              instruction_id, transform_id)

    test_task_helper.process_encoded(mocked_op, data)
    self.assertEquals(mocked_op.decoded, [])
    expected_wrapped_values = defaultdict(list)
    for decode, raw in zip(test_elems, raw_bytes):
      expected_wrapped_values[transform_id].append((decode.value, raw))
    self.assertItemsEqual(test_task_helper.wrapped_values, expected_wrapped_values)

  def test_process_normally_without_task_worker(self):
    """
    Test that when input data doesn't consists of TaskableValue, it is processed
    not using task worker but normally via DataInputOperation's process.
    """
    test_coder = self._get_test_int_coder()
    mocked_op = _MockDataInputOperation(test_coder)

    test_elems = self._prep_elements(range(5))
    instruction_id = 'test_instruction_2'
    transform_id = 'test_transform_1'

    test_task_helper = task_worker.BundleProcessorTaskHelper(instruction_id)
    _, data = self._prep_encoded_data(test_coder, test_elems, instruction_id,
                                      transform_id)

    test_task_helper.process_encoded(mocked_op, data)

    # when processed normally, it will use the DataInputOperation to process,
    # so it will be recorded in the `decoded` list
    self.assertEquals(mocked_op.decoded, test_elems)

  @mock.patch.object(handlers, 'MAX_TASK_WORKER_RETRY', 2)
  @mock.patch('__main__._MockTaskWorkerHandler.execute',
              side_effect=handlers.TaskWorkerProcessBundleError('test'))
  @mock.patch('__main__._MockTaskWorkerHandler.start_worker')
  @mock.patch('__main__._MockTaskWorkerHandler.stop_worker')
  def test_exceed_max_retries(self, unused_mock_stop, unused_mock_start,
                              mock_execute):
    """
    Test the scenario when task worker fails exceed max retries.
    """
    test_coder = self._get_test_pickle_coder()
    mocked_op = _MockDataInputOperation(test_coder)

    test_coder = self._get_test_pickle_coder()

    test_elems = self._prep_elements(
      [handlers.TaskableValue(i, 'unittest') for i in range(2)]
    )
    instruction_id = 'test_instruction_3'
    transform_id = 'test_transform_1'

    test_task_helper = task_worker.BundleProcessorTaskHelper(instruction_id)
    _, data = self._prep_encoded_data(test_coder, test_elems, instruction_id,
                                      transform_id)
    test_task_helper.process_encoded(mocked_op, data)

    dummy_process_bundle_descriptor = prep_bundle_processor_descriptor(1)
    dummy_data_channel_factory = GrpcClientDataChannelFactory()
    dummy_state_handler = _MockCachingStateHandler(None, None)

    with self.assertRaises(RuntimeError):
      test_task_helper.process_bundle_with_task_workers(
        dummy_state_handler,
        dummy_data_channel_factory,
        dummy_process_bundle_descriptor
      )

    # num(elems) * MAX_TASK_WORKER_RETRY
    self.assertEquals(mock_execute.call_count, 4)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
