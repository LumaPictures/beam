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

from __future__ import absolute_import
from __future__ import division

import collections
import logging
import queue
import os
import sys
import threading
from builtins import object
from concurrent import futures
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union

import grpc
from future.utils import raise_

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_task_worker_pb2
from apache_beam.portability.api import beam_task_worker_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability.fn_api_runner.worker_handlers import ControlConnection
from apache_beam.runners.portability.fn_api_runner.worker_handlers import ControlFuture
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
from apache_beam.runners.portability.fn_api_runner.worker_handlers import GrpcWorkerHandler
from apache_beam.runners.portability.fn_api_runner.worker_handlers import GrpcStateServicer
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandler
from apache_beam.runners.worker.bundle_processor import BundleProcessor
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.data_plane import _GrpcDataChannel
from apache_beam.runners.worker.data_plane import ClosableOutputStream
from apache_beam.runners.worker.data_plane import DataChannelFactory
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor

if TYPE_CHECKING:
  from apache_beam.portability.api import beam_provision_api_pb2
  from apache_beam.runners.portability.fn_api_runner import ExtendedProvisionInfo
  from apache_beam.runners.worker.data_plane import DataChannelFactory
  from apache_beam.runners.worker.sdk_worker import CachingStateHandler
  from apache_beam.transforms.environments import Environment

ENTRY_POINT_NAME = 'apache_beam_task_workers_plugins'
MAX_TASK_WORKERS = 300
MAX_TASK_WORKER_RETRY = 10

Taskable = Union[
  'TaskableValue',
  List['TaskableValue'],
  Tuple['TaskableValue', ...],
  Set['TaskableValue']]


class TaskableValue(object):
  """
  Value that can be distributed to TaskWorkers as tasks.

  Has the original value, and TaskProperties that specifies how the task will be
  generated."""

  def __init__(self,
               value,  # type: Any
               urn,  # type: str
               env=None,  # type: Optional[Environment]
               payload=None  # type: Optional[Any]
              ):
    # type: (...) -> None
    """
     Args:
       value: The wrapped element
       urn: id of the task worker handler
       env : Environment for the task to be run in
       payload : Payload containing settings for the task worker handler
     """
    self.value = value
    self.urn = urn
    self.env = env
    self.payload = payload


class TaskWorkerProcessBundleError(Exception):
  """
  Error thrown when TaskWorker fails to process_bundle.

  Errors encountered when task worker is processing bundle can be retried, up
  till max retries defined by ``MAX_TASK_WORKER_RETRY``.
  """


class TaskWorkerTerminatedError(Exception):
  """
  Error thrown when TaskWorker terminated before it finished working.

  Custom TaskWorkerHandlers can choose to terminate a task but not
  affect the whole bundle by setting ``TaskWorkerHandler.alive`` to False,
  which will cause this error to be thrown.
  """


class TaskWorkerHandler(GrpcWorkerHandler):
  """
  Abstract base class for TaskWorkerHandler for a task worker,

  A TaskWorkerHandler is created for each TaskableValue.

  Subclasses must override ``start_remote`` to modify how remote task worker is
  started, and register task properties type when defining a subclass.
  """

  _known_urns = {}  # type: Dict[str, Type[TaskWorkerHandler]]

  def __init__(self,
               state,  # type: CachingStateHandler
               provision_info,  # type: Union[beam_provision_api_pb2.ProvisionInfo, ExtendedProvisionInfo]
               grpc_server,  # type: TaskGrpcServer
               environment,  # type: Environment
               task_payload,  # type: Any
               credentials=None,  # type: Optional[str]
               worker_id=None  # type: Optional[str]
              ):
    # type: (...) -> None
    self._grpc_server = grpc_server

    # we are manually doing init instead of calling GrpcWorkerHandler's init
    # because we want to override worker_id and we don't want extra
    # ControlConnection to be established
    WorkerHandler.__init__(self, grpc_server.control_handler,
                           grpc_server.data_plane_handler, state,
                           provision_info)
    # override worker_id if provided
    if worker_id:
      self.worker_id = worker_id

    self.control_address = self.port_from_worker(self._grpc_server.control_port)
    self.logging_address = self.port_from_worker(
      self.get_port_from_env_var('LOGGING_API_SERVICE_DESCRIPTOR'))
    self.artifact_address = self.port_from_worker(
        self.get_port_from_env_var('ARTIFACT_API_SERVICE_DESCRIPTOR'))
    self.provision_address = self.port_from_worker(
        self.get_port_from_env_var('PROVISION_API_SERVICE_DESCRIPTOR'))

    self.control_conn = self._grpc_server.control_handler.get_conn_by_worker_id(
      self.worker_id)

    self.environment = environment
    self.task_payload = task_payload
    self.credentials = credentials
    self.alive = True

  def host_from_worker(self):
    # type: () -> str
    import socket
    return socket.getfqdn()

  def get_port_from_env_var(self, env_var):
    # type: (str) -> str
    """Extract the service port for a given environment variable."""
    from google.protobuf import text_format
    endpoint = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ[env_var], endpoint)
    return endpoint.url.split(':')[-1]

  @staticmethod
  def load_plugins():
    # type: () -> None
    import entrypoints
    for name, entry_point in entrypoints.get_group_named(
        ENTRY_POINT_NAME).iteritems():
      logging.info('Loading entry point: {}'.format(name))
      entry_point.load()

  @classmethod
  def register_urn(cls, urn, constructor=None):
    def register(constructor):
      cls._known_urns[urn] = constructor
      return constructor
    if constructor:
      return register(constructor)
    else:
      return register

  @classmethod
  def create(cls,
             state,  # type: CachingStateHandler
             provision_info,  # type: Union[beam_provision_api_pb2.ProvisionInfo, ExtendedProvisionInfo]
             grpc_server,  # type: TaskGrpcServer
             taskable_value,  # type: TaskableValue
             credentials=None,  # type: Optional[str]
             worker_id=None  # type: Optional[str]
            ):
    # type: (...) -> TaskWorkerHandler
    constructor = cls._known_urns[taskable_value.urn]
    return constructor(state, provision_info, grpc_server,
                       taskable_value.env, taskable_value.payload,
                       credentials=credentials, worker_id=worker_id)

  def start_worker(self):
    # type: () -> None
    self.start_remote()

  def start_remote(self):
    # type: () -> None
    """Start up a remote TaskWorker to process the current element.

    Subclass should implement this."""
    raise NotImplementedError

  def stop_worker(self):
    # type: () -> None
    # send shutdown request
    future = self.control_conn.push(
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=self.worker_id,
        shutdown=beam_task_worker_pb2.ShutdownRequest()))
    response = future.get()
    if response.error:
      logging.warning('Error stopping worker: {}'.format(self.worker_id))

    # close control conn after stop worker
    self.control_conn.close()

  def _get_future(self, future, interval=0.5):
    # type: (ControlFuture, float) -> beam_task_worker_pb2.TaskInstructionResponse
    result = None
    while self.alive:
      result = future.get(timeout=interval)
      if result:
        break

    # if the handler is not alive, meaning task worker is stopped before
    # finishing processing, raise ``TaskWorkerTerminatedError``
    if result is None:
      raise TaskWorkerTerminatedError()

    return result

  def execute(self,
              data_channel_factory,  # type: ProxyGrpcClientDataChannelFactory
              process_bundle_descriptor  # type: beam_fn_api_pb2.ProcessBundleDescriptor
             ):
    # type: (...) -> Tuple[List[beam_fn_api_pb2.DelayedBundleApplication], bool]
    """Main entry point of the task execution cycle of a ``TaskWorkerHandler``.

    It will first issue a create request, and wait for the remote bundle
    processor to be created; Then it will issue the process bundle request, and
    wait for the result. If there's error occurred when processing bundle,
    ``TaskWorkerProcessBundleError`` will be raised.
    """
    # wait for remote bundle processor to be created
    create_future = self.control_conn.push(
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=self.worker_id,
        create=beam_task_worker_pb2.CreateRequest(
          process_bundle_descriptor=process_bundle_descriptor,
          state_handler_endpoint=endpoints_pb2.ApiServiceDescriptor(
            url=self._grpc_server.state_address),
          data_factory=beam_task_worker_pb2.GrpcClientDataChannelFactory(
            credentials=self.credentials,
            worker_id=data_channel_factory.worker_id,
            transmitter_url=data_channel_factory.transmitter_url))))
    self._get_future(create_future)

    # process bundle
    process_future = self.control_conn.push(
      beam_task_worker_pb2.TaskInstructionRequest(
        instruction_id=self.worker_id,
        process_bundle=beam_task_worker_pb2.ProcessorProcessBundleRequest())
    )
    response = self._get_future(process_future)
    if response.error:
      # raise here so this task can be retried
      raise TaskWorkerProcessBundleError()
    else:
      delayed_applications = response.process_bundle.delayed_applications
      require_finalization = response.process_bundle.require_finalization

      self.stop_worker()
      return delayed_applications, require_finalization

  def reset(self):
    # type: () -> None
    """This is used to retry a failed task."""
    self.control_conn.reset()


class TaskGrpcServer(object):
  """
  A collection of grpc servicers that handle communication between a
  ``TaskWorker`` and ``TaskWorkerHandler``.

  Contains three servers:
  - a control server hosting ``TaskControlService``
  - a data server hosting ``TaskFnDataService``
  - a state server hosting ``TaskStateService``

  This is shared by all TaskWorkerHandlers generated by one bundle.
  """

  _DEFAULT_SHUTDOWN_TIMEOUT_SECS = 5

  def __init__(self,
               state_handler,  # type: CachingStateHandler
               max_workers,  # type: int
               data_store,  # type: Mapping[str, List[beam_fn_api_pb2.Elements.Data]]
               data_channel_factory,  # type: DataChannelFactory
               instruction_id  # type: str
              ):
    # type: (...) -> None
    self.state_handler = state_handler
    self.max_workers = max_workers
    self.control_server = grpc.server(
      futures.ThreadPoolExecutor(max_workers=self.max_workers))
    self.control_port = self.control_server.add_insecure_port('[::]:0')
    self.control_address = '%s:%s' % (self.get_host_name(), self.control_port)

    # Options to have no limits (-1) on the size of the messages
    # received or sent over the data plane. The actual buffer size
    # is controlled in a layer above.
    no_max_message_sizes = [("grpc.max_receive_message_length", -1),
                            ("grpc.max_send_message_length", -1)]
    self.data_server = grpc.server(
      futures.ThreadPoolExecutor(max_workers=self.max_workers),
      options=no_max_message_sizes)
    self.data_port = self.data_server.add_insecure_port('[::]:0')
    self.data_address = '%s:%s' % (self.get_host_name(), self.data_port)

    self.state_server = grpc.server(
      futures.ThreadPoolExecutor(max_workers=self.max_workers),
      options=no_max_message_sizes)
    self.state_port = self.state_server.add_insecure_port('[::]:0')
    self.state_address = '%s:%s' % (self.get_host_name(), self.state_port)

    self.control_handler = TaskControlServicer()
    beam_task_worker_pb2_grpc.add_TaskControlServicer_to_server(
      self.control_handler, self.control_server)
    # TODO: When we add provision / staging service, it needs to be added to the
    #  control server too

    self.data_plane_handler = TaskFnDataServicer(data_store,
                                                 data_channel_factory,
                                                 instruction_id)
    beam_task_worker_pb2_grpc.add_TaskFnDataServicer_to_server(
      self.data_plane_handler, self.data_server)

    beam_fn_api_pb2_grpc.add_BeamFnStateServicer_to_server(
      TaskStateServicer(self.state_handler, instruction_id,
                        state_handler._context.bundle_cache_token),
      self.state_server)

    logging.info('starting control server on port %s', self.control_port)
    logging.info('starting data server on port %s', self.data_port)
    logging.info('starting state server on port %s', self.state_port)
    self.state_server.start()
    self.data_server.start()
    self.control_server.start()

  @staticmethod
  def get_host_name():
    # type: () -> str
    import socket
    return socket.getfqdn()

  def close(self):
    # type: () -> None
    self.control_handler.done()
    to_wait = [
      self.control_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
      self.data_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
      self.state_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
    ]
    for w in to_wait:
      w.wait()


# =============
# Control Plane
# =============
class TaskWorkerConnection(ControlConnection):
  """The control connection between a TaskWorker and a TaskWorkerHandler.

  TaskWorkerHandler push InstructionRequests to _push_queue, and receives
  InstructionResponses from TaskControlServicer.
  """

  _lock = threading.Lock()

  def __init__(self):
    self._push_queue = queue.Queue()
    self._input = None
    self._futures_by_id = {}  # type: Dict[Any, ControlFuture]
    self._read_thread = threading.Thread(
      name='bundle_processor_control_read', target=self._read)
    self._state = TaskControlServicer.UNSTARTED_STATE
    # marks current TaskConnection as in a state of retrying after failure
    self._retrying = False

  def _read(self):
    # type: () -> None
    for data in self._input:
      self._futures_by_id.pop(data.WhichOneof('response')).set(data)

  def push(self,
           req  # type: Union[TaskControlServicer._DONE_MARKER, beam_task_worker_pb2.TaskInstructionRequest]
          ):
    # type: (...) -> Optional[ControlFuture]
    if req == TaskControlServicer._DONE_MARKER:
      self._push_queue.put(req)
      return None
    if not req.instruction_id:
      raise RuntimeError(
        'TaskInstructionRequest has to have instruction id!')
    future = ControlFuture(req.instruction_id)
    self._futures_by_id[req.WhichOneof('request')] = future
    self._push_queue.put(req)
    return future

  def set_inputs(self, input):
    with TaskWorkerConnection._lock:
      if self._input and not self._retrying:
        raise RuntimeError('input is already set.')
      self._input = input
      self._read_thread.start()
      self._state = TaskControlServicer.STARTED_STATE
      self._retrying = False

  def close(self):
    # type: () -> None
    with TaskWorkerConnection._lock:
      if self._state == TaskControlServicer.STARTED_STATE:
        self.push(TaskControlServicer._DONE_MARKER)
        self._read_thread.join()
      self._state = TaskControlServicer.DONE_STATE

  def reset(self):
    # type: () -> None
    self.close()
    self.__init__()
    self._retrying = True


class TaskControlServicer(beam_task_worker_pb2_grpc.TaskControlServicer):

  _lock = threading.Lock()

  UNSTARTED_STATE = 'unstarted'
  STARTED_STATE = 'started'
  DONE_STATE = 'done'

  _DONE_MARKER = object()

  def __init__(self):
    # type: () -> None
    self._state = self.UNSTARTED_STATE
    self._connections_by_worker_id = collections.defaultdict(
      TaskWorkerConnection)

  def get_conn_by_worker_id(self, worker_id):
    # type: (str) -> TaskWorkerConnection
    with self._lock:
      result = self._connections_by_worker_id[worker_id]
      return result

  def Control(self, request_iterator, context):
    with self._lock:
      if self._state == self.DONE_STATE:
        return
      else:
        self._state = self.STARTED_STATE
    worker_id = dict(context.invocation_metadata()).get('worker_id')
    if not worker_id:
      raise RuntimeError('Connection does not have worker id.')
    conn = self.get_conn_by_worker_id(worker_id)
    conn.set_inputs(request_iterator)

    while True:
      to_push = conn.get_req()
      if to_push is self._DONE_MARKER:
        return
      yield to_push

  def done(self):
    # type: () -> None
    self._state = self.DONE_STATE


# ==========
# Data Plane
# ==========
class ProxyGrpcClientDataChannelFactory(DataChannelFactory):
  """A factory for ``ProxyGrpcClientDataChannel``.

  No caching behavior here because we are starting each data channel on
  different location."""

  def __init__(self, transmitter_url, credentials=None, worker_id=None):
    # type: (str, Optional[str], Optional[str]) -> None
    # These two are not private attributes because it was used in
    # ``TaskWorkerHandler.execute`` when issuing TaskInstructionRequest
    self.transmitter_url = transmitter_url
    self.worker_id = worker_id

    self._credentials = credentials

  def create_data_channel(self, remote_grpc_port):
    # type: (beam_fn_api_pb2.RemoteGrpcPort) -> ProxyGrpcClientDataChannel
    channel_options = [("grpc.max_receive_message_length", -1),
                       ("grpc.max_send_message_length", -1)]
    if self._credentials is None:
      grpc_channel = GRPCChannelFactory.insecure_channel(
        self.transmitter_url, options=channel_options)
    else:
      grpc_channel = GRPCChannelFactory.secure_channel(
        self.transmitter_url, self._credentials, options=channel_options)
    return ProxyGrpcClientDataChannel(
      remote_grpc_port.api_service_descriptor.url,
      beam_task_worker_pb2_grpc.TaskFnDataStub(grpc_channel))

  def close(self):
    # type: () -> None
    pass


class ProxyGrpcClientDataChannel(_GrpcDataChannel):
  """DataChannel wrapping the client side of a TaskFnDataService connection."""

  def __init__(self, client_url, proxy_stub):
    # type: (str, beam_task_worker_pb2_grpc.TaskFnDataStub) -> None
    super(ProxyGrpcClientDataChannel, self).__init__()
    self.client_url = client_url
    self.proxy_stub = proxy_stub

  def input_elements(self,
                     instruction_id,  # type: str
                     expected_transforms,  # type: List[str]
                     abort_callback=None  # type: Optional[Callable[[], bool]]
                    ):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements.Data]
    req = beam_task_worker_pb2.ReceiveRequest(
      instruction_id=instruction_id,
      client_data_endpoint=self.client_url)
    done_transforms = []
    abort_callback = abort_callback or (lambda: False)

    for data in self.proxy_stub.Receive(req):
      if self._closed:
        raise RuntimeError('Channel closed prematurely.')
      if abort_callback():
        return
      if self._exc_info:
        t, v, tb = self._exc_info
        raise_(t, v, tb)
      if not data.data and data.transform_id in expected_transforms:
        done_transforms.append(data.transform_id)
      else:
        assert data.transform_id not in done_transforms
        yield data
      if len(done_transforms) >= len(expected_transforms):
        return

  def output_stream(self, instruction_id, transform_id):
    # type: (str, str) -> ClosableOutputStream

    def _add_to_send_queue(data):
      if data:
        self.proxy_stub.Send(beam_task_worker_pb2.SendRequest(
          instruction_id=instruction_id,
          data=beam_fn_api_pb2.Elements.Data(
            instruction_id=instruction_id,
            transform_id=transform_id,
            data=data),
          client_data_endpoint=self.client_url
        ))

    def close_callback(data):
      _add_to_send_queue(data)
      # no need to send empty bytes to signal end of processing here, because
      # when the whole bundle finishes, the bundle processor original output
      # stream will send that to runner

    return ClosableOutputStream(
      close_callback, flush_callback=_add_to_send_queue)


class TaskFnDataServicer(beam_task_worker_pb2_grpc.TaskFnDataServicer):
  """Implementation of BeamFnDataTransmitServicer for any number of clients."""

  def __init__(self,
               data_store,  # type: Mapping[str, List[beam_fn_api_pb2.Elements.Data]]
               orig_data_channel_factory,  # type: DataChannelFactory
               instruction_id  # type: str
              ):
    # type: (...) -> None
    self.data_store = data_store
    self.orig_data_channel_factory = orig_data_channel_factory
    self.orig_instruction_id = instruction_id
    self._orig_data_channel = None  # type: Optional[ProxyGrpcClientDataChannel]

  def _get_orig_data_channel(self, url):
    # type: (str) -> ProxyGrpcClientDataChannel
    remote_grpc_port = beam_fn_api_pb2.RemoteGrpcPort(
      api_service_descriptor=endpoints_pb2.ApiServiceDescriptor(url=url))
    # the data channel is cached by url
    return self.orig_data_channel_factory.create_data_channel(remote_grpc_port)

  def Receive(self, request, context=None):
    # type: (...) -> Iterator[beam_fn_api_pb2.Elements.Data]]
    data = self.data_store[request.instruction_id]
    for elem in data:
      yield elem

  def Send(self, request, context=None):
    # type: (...) -> beam_task_worker_pb2.SendResponse
    if self._orig_data_channel is None:
      self._orig_data_channel = self._get_orig_data_channel(
        request.client_data_endpoint)
    # We need to replace the instruction_id here with the original instruction
    # id, not the current one (which is the task worker id)
    request.data.instruction_id = self.orig_instruction_id
    if request.data.data:
      # only send when there's data, because it is signaling the runner side
      # worker handler that element of this has ended if it is empty, and we
      # want to send that when every task worker handler is finished
      self._orig_data_channel._to_send.put(request.data)
    return beam_task_worker_pb2.SendResponse()


# =====
# State
# =====
class TaskStateServicer(GrpcStateServicer):

  def __init__(self, state, instruction_id, cache_token):
    # type: (CachingStateHandler, str, Optional[str]) -> None
    self.instruction_id = instruction_id
    self.cache_token = cache_token
    super(TaskStateServicer, self).__init__(state)

  def State(self, request_stream, context=None):
    # type: (...) -> Iterator[beam_fn_api_pb2.StateResponse]
    # CachingStateHandler and GrpcStateHandler context is thread local, so we
    # need to set it here for each TaskWorker
    self._state._context.process_instruction_id = self.instruction_id
    self._state._context.cache_token = self.cache_token
    self._state._underlying._context.process_instruction_id = self.instruction_id

    # FIXME: This is not currently properly supporting state caching (currently
    #  state caching behavior only happens within python SDK, so runners like
    #  the FlinkRunner won't create the state cache anyways for now)
    for request in request_stream:
      request_type = request.WhichOneof('request')

      if request_type == 'get':
        data, continuation_token = self._state._underlying.get_raw(
          request.state_key, request.get.continuation_token)
        yield beam_fn_api_pb2.StateResponse(
          id=request.id,
          get=beam_fn_api_pb2.StateGetResponse(
            data=data, continuation_token=continuation_token))
      elif request_type == 'append':
        self._state._underlying.append_raw(request.state_key,
                                           request.append.data)
        yield beam_fn_api_pb2.StateResponse(
          id=request.id,
          append=beam_fn_api_pb2.StateAppendResponse())
      elif request_type == 'clear':
        self._state._underlying.clear(request.state_key)
        yield beam_fn_api_pb2.StateResponse(
          id=request.id,
          clear=beam_fn_api_pb2.StateClearResponse())
      else:
        raise NotImplementedError('Unknown state request: %s' % request_type)


class BundleProcessorTaskWorker(object):
  """
  The remote task worker that communicates with the SDK worker to do the
  actual work of processing bundles.

  The BundleProcessor will detect inputs and see if there is TaskableValue, and
  if there is and the BundleProcessor is set to "use task worker", then
  BundleProcessorTaskHelper will create a TaskWorkerHandler that this class
  communicates with.

  This class creates a BundleProcessor and receives TaskInstructionRequests and
  sends back respective responses via the grpc channels connected to the control
  endpoint;
  """

  REQUEST_PREFIX = '_request_'
  _lock = threading.Lock()

  def __init__(self, worker_id, server_url, credentials=None):
    # type: (str, str, Optional[str]) -> None
    """Initialize a BundleProcessorTaskWorker. Lives remotely.

    It will create a BundleProcessor with the provide information and process
    the requests using the BundleProcessor created.

    Args:
      worker_id: the worker id of current task worker
      server_url: control service url for the TaskGrpcServer
      credentials: credentials to use when creating client
    """
    self.worker_id = worker_id
    self._credentials = credentials
    self._responses = queue.Queue()
    self._alive = None  # type: Optional[bool]
    self._bundle_processor = None  # type: Optional[BundleProcessor]
    self._exc_info = None
    self.stub = self._create_stub(server_url)

  @classmethod
  def execute(cls, worker_id, server_url, credentials=None):
    # type: (str, str, Optional[str]) -> None
    """Instantiate a BundleProcessorTaskWorker and start running.

    If there's error, it will be raised here so it can be reflected to user.

    Args:
      worker_id: worker id for the BundleProcessorTaskWorker
      server_url: control service url for the TaskGrpcServer
      credentials: credentials to use when creating client
    """
    self = cls(worker_id, server_url, credentials=credentials)
    self.run()

    # raise the error here, so user knows there's a failure and could retry
    if self._exc_info:
      t, v, tb = self._exc_info
      raise_(t, v, tb)

  def _create_stub(self, server_url):
    # type: (str) -> beam_task_worker_pb2_grpc.TaskControlStub
    """Create the TaskControl client."""
    channel_options = [("grpc.max_receive_message_length", -1),
                       ("grpc.max_send_message_length", -1)]
    if self._credentials is None:
      channel = GRPCChannelFactory.insecure_channel(
        server_url,
        options=channel_options)
    else:
      channel = GRPCChannelFactory.secure_channel(server_url,
                                                  self._credentials,
                                                  options=channel_options)

    # add instruction_id to grpc channel
    channel = grpc.intercept_channel(
      channel,
      WorkerIdInterceptor(self.worker_id))

    return beam_task_worker_pb2_grpc.TaskControlStub(channel)

  def do_instruction(self, request):
    # type: (beam_task_worker_pb2.TaskInstructionRequest) -> beam_task_worker_pb2.TaskInstructionResponse
    """Process the requests with the corresponding method."""
    request_type = request.WhichOneof('request')
    if request_type:
      return getattr(self, self.REQUEST_PREFIX + request_type)(
        getattr(request, request_type))
    else:
      raise NotImplementedError

  def run(self):
    # type: () -> None
    """Start the full running life cycle for a task worker.

    It send TaskWorkerInstructionResponse to TaskWorkerHandler, and wait for
    TaskWorkerInstructionRequest. This service is bidirectional.
    """
    no_more_work = object()
    self._alive = True

    def get_responses():
      while True:
        response = self._responses.get()
        if response is no_more_work:
          return
        if response:
          yield response

    try:
      for request in self.stub.Control(get_responses()):
        self._responses.put(self.do_instruction(request))
    finally:
      self._alive = False

    self._responses.put(no_more_work)
    logging.info('Done consuming work.')

  def _request_create(self, request):
    # type: (beam_task_worker_pb2.TaskInstructionRequest) -> beam_task_worker_pb2.TaskInstructionResponse
    """Create a BundleProcessor based on the request.

    Should be the first request received from the handler.
    """
    from apache_beam.runners.worker.sdk_worker import \
      GrpcStateHandlerFactory

    credentials = None
    if request.data_factory.credentials._credentials:
      credentials = grpc.ChannelCredentials(
        request.data_factory.credentials._credentials)
    logging.debug('Credentials: {!r}'.format(credentials))

    worker_id = request.data_factory.worker_id
    transmitter_url = request.data_factory.transmitter_url
    state_handler_endpoint = request.state_handler_endpoint
    # FIXME: Add support for Caching later
    state_factory = GrpcStateHandlerFactory(StateCache(0), credentials)
    state_handler = state_factory.create_state_handler(
      state_handler_endpoint)
    data_channel_factory = ProxyGrpcClientDataChannelFactory(
      transmitter_url, credentials, worker_id
    )

    self._bundle_processor = BundleProcessor(
      request.process_bundle_descriptor,
      state_handler,
      data_channel_factory
    )
    return beam_task_worker_pb2.TaskInstructionResponse(
      create=beam_task_worker_pb2.CreateResponse(),
      instruction_id=self.worker_id
    )

  def _request_process_bundle(self,
                              request  # type: beam_task_worker_pb2.TaskInstructionRequest
                             ):
    # type: (...) -> beam_task_worker_pb2.TaskInstructionResponse
    """Process bundle using the bundle processor based on the request."""
    error = None

    try:
      # FIXME: Update this to use the cache_tokens properly
      with self._bundle_processor.state_handler._underlying.process_instruction_id(
          self.worker_id):
        delayed_applications, require_finalization = \
          self._bundle_processor.process_bundle(self.worker_id,
                                                use_task_worker=False)
    except Exception as e:
      # we want to propagate the error back to the TaskWorkerHandler, so that
      # it will raise `TaskWorkerProcessBundleError` which allows for requeue
      # behavior (up until MAX_TASK_WORKER_RETRY number of retries)
      error = e.message
      self._exc_info = sys.exc_info()
      delayed_applications = []
      require_finalization = False

    return beam_task_worker_pb2.TaskInstructionResponse(
      process_bundle=beam_task_worker_pb2.ProcessorProcessBundleResponse(
        delayed_applications=delayed_applications,
        require_finalization=require_finalization),
      instruction_id=self.worker_id,
      error=error
    )

  def _request_shutdown(self, request):
    # type: (beam_task_worker_pb2.TaskInstructionRequest) -> beam_task_worker_pb2.TaskInstructionResponse
    """Shutdown the bundleprocessor."""
    error = None
    try:
      # shut down state handler here because it is not created by the state
      # handler factory thus won't be closed automatically
      self._bundle_processor.state_handler.done()
      self._bundle_processor.shutdown()
    except Exception as e:
      error = e.message
    finally:
      return beam_task_worker_pb2.TaskInstructionResponse(
        shutdown=beam_task_worker_pb2.ShutdownResponse(),
        instruction_id=self.worker_id,
        error=error
      )


class BundleProcessorTaskHelper(object):
  """
  A helper object that is used by a BundleProcessor while processing bundle.

  Delegates TaskableValues to TaskWorkers, if enabled.

  It can process TaskableValue using TaskWorkers if inspected, and kept the
  default behavior if specified to not use TaskWorker, or there's no
  TaskableValue found in this bundle.

  To utilize TaskWorkers, BundleProcessorTaskHelper will split up the input
  bundle into tasks based on the wrapped TaskableValue's payload, and create a
  TaskWorkerHandler for each task.
  """

  def __init__(self, instruction_id, wrapped_values):
    # type: (str, DefaultDict[str, List[Tuple[Any, bytes]]]) -> None
    """Initialize a BundleProcessorTaskHelper object.

    Args:
      instruction_id: the instruction_id of the bundle that the
        BundleProcessor is processing
      wrapped_values: The mapping of transform id to raw and encoded data.

    """
    self.instruction_id = instruction_id
    self.wrapped_values = wrapped_values

  def split_taskable_values(self):
    # type: () -> Tuple[DefaultDict[str, List[Any]], DefaultDict[str, List[beam_fn_api_pb2.Elements.Data]]]
    """Split TaskableValues into tasks and pair it with worker.

    Also put the raw bytes along with worker id for data dispatching by data
    plane handler.
    """
    # TODO: Come up with solution on how this can be dynamically changed
    #  could use window
    splitted = collections.defaultdict(list)
    data_store = collections.defaultdict(list)
    worker_count = 0
    for ptransform_id, values in self.wrapped_values.iteritems():
      for decoded, raw in values:
        worker_id = 'worker_{}'.format(worker_count)
        splitted[worker_id].append(decoded)
        data_store[worker_id].append(beam_fn_api_pb2.Elements.Data(
          transform_id=ptransform_id,
          data=raw,
          instruction_id=worker_id
        ))
        worker_count += 1

    return splitted, data_store

  def _start_task_grpc_server(self,
                              max_workers,  # type: int
                              data_store,  # type: Mapping[str, List[beam_fn_api_pb2.Elements.Data]]
                              state_handler,  # type: CachingStateHandler
                              data_channel_factory  # type: DataChannelFactory
                             ):
    # type:(...) -> TaskGrpcServer
    """Start up TaskGrpcServer.

    Args:
      max_workers: number of max worker
      data_store: stored data of worker id and the raw and decoded values for
        the worker to process as inputs
      state_handler: state handler of current BundleProcessor
      data_channel_factory: data channel factory of current BundleProcessor
    """
    return TaskGrpcServer(state_handler, max_workers, data_store,
                          data_channel_factory, self.instruction_id)

  @staticmethod
  def get_default_task_env(process_bundle_descriptor):
    # type:(beam_fn_api_pb2.ProcessBundleDescriptor) -> Optional[Environment]
    """Get the current running beam Environment class.

    Used as the default for the task worker.

    Args:
      process_bundle_descriptor: the ProcessBundleDescriptor proto
    """
    from apache_beam.runners.portability.fn_api_runner.translations import \
      PAR_DO_URNS
    from apache_beam.transforms.environments import Environment

    # find a ParDo xform in this stage
    pardo = None
    for _, xform in process_bundle_descriptor.transforms.iteritems():
      if xform.spec.urn in PAR_DO_URNS:
        pardo = xform
        break

    if pardo is None:
      # don't set the default task env if no ParDo is found
      # FIXME: Use the pipeline default env here?
      return None

    env_proto = process_bundle_descriptor.environments.get(
      pardo.environment_id)

    return Environment.from_runner_api(env_proto, None)

  def process_bundle_with_task_workers(self,
                                       state_handler,  # type: CachingStateHandler
                                       data_channel_factory,  # type: DataChannelFactory
                                       process_bundle_descriptor  # type: beam_fn_api_pb2.ProcessBundleDescriptor
                                       ):
    # type: (...) -> Tuple[List[beam_fn_api_pb2.DelayedBundleApplication], bool]
    """Main entry point for task worker system.

    Starts up a group of TaskWorkerHandlers, dispatches tasks and waits for them
    to finish.

    Fails if any TaskWorker exceeds maximum retries.

    Args:
      state_handler: state handler of current BundleProcessor
      data_channel_factory: data channel factory of current BundleProcessor
      process_bundle_descriptor: a description of the stage that this
        ``BundleProcessor``is to execute.
    """
    default_env = self.get_default_task_env(process_bundle_descriptor)
    # start up grpc server
    splitted_elements, data_store = self.split_taskable_values()
    num_task_workers = len(splitted_elements.items())
    if num_task_workers > MAX_TASK_WORKERS:
      logging.warning(
        'Number of element exceeded MAX_TASK_WORKERS ({})'.format(
          MAX_TASK_WORKERS))
      num_task_workers = MAX_TASK_WORKERS
    server = self._start_task_grpc_server(num_task_workers, data_store,
                                          state_handler, data_channel_factory)

    # create TaskWorkerHandlers
    task_worker_handlers = []
    # FIXME: leaving out provision info for now, it should come from
    #  Environment
    provision_info = None
    for worker_id, elem in splitted_elements.iteritems():
      taskable_value = get_taskable_value(elem)
      # set the env to default env if there is
      if taskable_value.env is None and default_env:
        taskable_value.env = default_env

      task_worker_handler = TaskWorkerHandler.create(
        state_handler, provision_info, server, taskable_value,
        credentials=data_channel_factory._credentials, worker_id=worker_id)
      task_worker_handlers.append(task_worker_handler)
      task_worker_handler.start_worker()

    def _execute(handler):
      """
      This is the method that runs in the thread pool representing a working
      TaskHandler.
      """
      worker_data_channel_factory = ProxyGrpcClientDataChannelFactory(
        server.data_address,
        credentials=data_channel_factory._credentials,
        worker_id=task_worker_handler.worker_id)
      counter = 0
      while True:
        try:
          counter += 1
          return handler.execute(worker_data_channel_factory,
                                 process_bundle_descriptor)
        except TaskWorkerProcessBundleError as e:
          if counter >= MAX_TASK_WORKER_RETRY:
            logging.error('Task Worker has exceeded max retries!')
            handler.stop_worker()
            raise
          # retry if task worker failed to process bundle
          handler.reset()
          continue
        except TaskWorkerTerminatedError as e:
          # This error is thrown only when TaskWorkerHandler is terminated
          # before it finished processing
          logging.warning('TaskWorker terminated prematurely.')
          raise

    # start actual processing of splitted bundle
    merged_delayed_applications = []
    bundle_require_finalization = False
    with futures.ThreadPoolExecutor(max_workers=num_task_workers) as executor:
      try:
        for delayed_applications, require_finalization in executor.map(
            _execute, task_worker_handlers):

          if delayed_applications is None:
            raise RuntimeError('Task Worker failed to process task.')
          merged_delayed_applications.extend(delayed_applications)
          # if any elem requires finalization, set it to True
          if not bundle_require_finalization and require_finalization:
            bundle_require_finalization = True
      except TaskWorkerProcessBundleError:
        raise RuntimeError('Task Worker failed to process task.')
      except TaskWorkerTerminatedError:
        # This error is thrown only when TaskWorkerHandler is terminated before
        # it finished processing, this is only possible if
        # `TaskWorkerHandler.alive` is manually set to False by custom user
        # defined monitoring function, which user would trigger if they want
        # to terminate the task;
        # In that case, we want to continue on and not hold the whole bundle
        # by the tasks that user manually terminated.
        pass

    return merged_delayed_applications, bundle_require_finalization


@TaskWorkerHandler.register_urn('local')
class LocalTaskWorkerHandler(TaskWorkerHandler):
  """TaskWorkerHandler that starts up task worker locally."""

  # FIXME: create a class-level thread pool to restrict the number of threads

  def start_remote(self):
    # type: () -> None
    """start a task worker local to the task worker handler."""
    obj = BundleProcessorTaskWorker(self.worker_id, self.control_address,
                                    self.credentials)
    run_thread = threading.Thread(target=obj.run)
    run_thread.daemon = True
    run_thread.start()


TaskWorkerHandler.load_plugins()


def get_taskable_value(decoded_value):
  # type: (Any) -> Optional[TaskableValue]
  """Check whether the given value contains taskable value.

  If taskable, return the TaskableValue

  Args:
    decoded_value: decoded value from raw input stream
  """
  # FIXME: Come up with a solution that's not so specific
  if isinstance(decoded_value, (list, tuple, set)):
    for val in decoded_value:
      result = get_taskable_value(val)
      if result:
        return result
  elif isinstance(decoded_value, TaskableValue):
    return decoded_value
  return None
