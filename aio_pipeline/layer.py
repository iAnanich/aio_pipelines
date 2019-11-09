import abc
import asyncio
import typing
import logging

from .event import OwnedEvent
from .node import AbstractNode, BaseNode, Node
from .state import STATES, State, WrongState

__all__ = (
    'BaseLayer', 'Layer',
    'SoloLayer', 'HeadLayer', 'MiddleLayer', 'TailLayer',
)


class AbstractLayer(metaclass=abc.ABCMeta):
    class DEFAULT:
        QUEUE: asyncio.Queue

    needs_next_layer: bool

    def __init__(self, nodes: typing.Collection[AbstractNode],
                 queue: asyncio.Queue):
        """
        :param queue: asyncio.Queue used for retrieving items.
        :param nodes: nodes meant to be run concurrently.
        """
        self._queue = queue
        self._nodes = tuple(nodes)

        self._state: State = STATES.IDLE
        self._next_layer: BaseLayer = None
        self._pipeline = None

        # events
        self._started_event = OwnedEvent(owner=self, name='layer started')
        self._going_to_stop_event = OwnedEvent(owner=self, name='layer going to stop')
        self._stopped_event = OwnedEvent(owner=self, name='layer stopped')
        self._aborting_event = OwnedEvent(owner=self, name='layer aborting')
        self._gracefully_stopping_event = OwnedEvent(owner=self, name='layer gracefully stopping')
        self._finalizer_lock = asyncio.Lock()

        # tasks
        self._running_task: asyncio.Task = None
        self._finalizer_task: asyncio.Task = None

    @property
    def queue(self) -> asyncio.Queue:
        return self._queue

    @property
    def nodes(self) -> typing.Tuple[AbstractNode, ...]:
        return self._nodes

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def state(self) -> State:
        return self._state

    @property
    def next_layer(self) -> 'BaseLayer':
        return self._next_layer

    @property
    def layer_stopped_event(self) -> asyncio.Event:
        return self._stopped_event

    @abc.abstractmethod
    def connect_next_layer(self, next_layer: 'AbstractLayer') -> None:
        """
        Bind's next_layer object to current object in order to use it's queue
        for forwarding items.
        :param next_layer: BaseLayer object, which is standing next to the current
        Layer.
        :return: None
        """
        pass

    @abc.abstractmethod
    def start(self) -> None:
        """
        Begins layer's work.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def stop(self, join_queue: bool = True) -> None:
        """
        Stops layer's work.
        :param join_queue: if True, will wait for queue to be completed.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def stop_at_event(self, event: asyncio.Event,
                            join_queue: bool = True) -> None:
        """
        Awaits on event to be set, than stops layer.
        :param event: asyncio.Event that will trigger that stop.
        :param join_queue: if True, will wait for queue to be completed.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def forward_item(self, obj: object) -> None:
        """
        Forward item to next layer.
        :param obj: any object.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def take_item(self) -> object:
        """
        Take item from previous layer.
        :return: any object
        """
        pass

    @abc.abstractmethod
    def done_item(self) -> None:
        """
        Notifies previous layer that taken item was processed.
        :return: None
        """
        pass

    @abc.abstractmethod
    def abort(self) -> None:
        """
        Sets event dedicated for stop of the layer without awaiting on pending items.
        :return: None
        """
        pass

    @abc.abstractmethod
    def stop_gracefully(self) -> None:
        """
        Sets event dedicated for graceful stop of the layer.
        :return: None
        """

    @abc.abstractmethod
    async def before_start(self) -> None:
        """
        Executes before starting concurrent nodes.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def before_stopping(self) -> None:
        """
        Executes before stopping concurrent nodes.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def after_stopped(self) -> None:
        """
        Executes just after last node was stopped.
        :return:  None
        """
        pass


class BaseLayer(AbstractLayer, metaclass=abc.ABCMeta):
    class DEFAULT:
        QUEUE = asyncio.Queue(maxsize=0)

    STATES = STATES

    needs_next_layer: bool = None

    def __init__(self, nodes: typing.Collection[BaseNode],
                 queue: asyncio.Queue = DEFAULT.QUEUE,
                 logger: logging.Logger = None):
        super().__init__(
            nodes=nodes,
            queue=queue,
        )

        self.log = self.logger = logger or logging.getLogger(self.__class__.__name__)

    def connect_next_layer(self, next_layer: 'BaseLayer') -> None:
        if not isinstance(next_layer, BaseLayer):
            raise TypeError
        self._next_layer = next_layer

    def bind_pipeline(self, pipeline) -> None:
        from .pipeline import BasePipeline
        if not isinstance(pipeline, BasePipeline):
            raise TypeError
        self._pipeline = pipeline

    async def start(self) -> None:
        if self._state != STATES.IDLE:
            raise WrongState

        if self.needs_next_layer and not self.next_layer:
            raise RuntimeError

        self.log.debug(f'Starting...')

        self._state = STATES.RUNNING
        self._started_event.set()

        await self.before_start()
        await self._start_runner_task()
        await self.stop(join_queue=False)

    async def stop(self, join_queue: bool = True) -> None:
        async with self._finalizer_lock:
            if self.state == STATES.STOPPED:
                return

            self.log.debug(f'Going to stop with join_queue={join_queue}')

            self._state = STATES.GOING_TO_STOP
            self._going_to_stop_event.set()

            if join_queue:
                await self._queue.join()
            await self.before_stopping()

            self._finalizer_task = asyncio.create_task(self._finish_runner_task())
            await self._finalizer_task

            await self.after_stopped()
            if not self._abort_at_event_task.done() and not self._abort_at_event_task.cancelled():
                self._abort_at_event_task.cancel()
            if not self._graceful_stop_at_event_task.done() and not self._graceful_stop_at_event_task.cancelled():
                self._graceful_stop_at_event_task.cancel()

            self._state = STATES.STOPPED
            self._stopped_event.set()

            self.log.info(f'Stopped.')

    # TODO: implement stop_gracefully

    async def _start_runner_task(self) -> None:
        async def gather_nodes():
            await asyncio.gather(
                *(
                    node.start(layer=self)
                    for node in self._nodes
                ),
                return_exceptions=True,
            )

        self._abort_at_event_task = asyncio.create_task(self.stop_at_event(
            event=self._aborting_event,
            join_queue=False,
        ))
        self._graceful_stop_at_event_task = asyncio.create_task(self.stop_at_event(
            event=self._gracefully_stopping_event,
            join_queue=True,
        ))
        self._running_task = asyncio.create_task(gather_nodes())

        self.log.info(f'Started.')
        await self._running_task

    async def _finish_runner_task(self) -> None:
        for node in self._nodes:
            await node.stop()

        await self._running_task

    async def stop_at_event(self, event: asyncio.Event,
                            join_queue: bool = True) -> None:
        await event.wait()
        await self.stop(join_queue=join_queue)

    async def forward_item(self, obj: object) -> None:
        await self.next_layer._queue.put(obj)

    async def forward_items(self, objs: [object, ...]) -> None:
        for obj in objs:
            await self.forward_item(obj)

    async def take_item(self) -> object:
        return await self._queue.get()

    def done_item(self) -> None:
        self._queue.task_done()

    def abort(self) -> None:
        self._aborting_event.set()

    def stop_gracefully(self) -> None:
        self._gracefully_stopping_event.set()

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} [{self.state}]>'

    async def before_start(self) -> None:
        pass

    async def before_stopping(self) -> None:
        pass

    async def after_stopped(self) -> None:
        pass


class Layer(BaseLayer, metaclass=abc.ABCMeta):
    class DEFAULT(BaseLayer.DEFAULT):
        NODES_AUTO_RERUN = False
        CONCURRENCY = 1
        QUEUE_MAX_SIZE = 0

    def __init__(self, nodes_auto_rerun: bool = DEFAULT.NODES_AUTO_RERUN,
                 concurrency: int = DEFAULT.CONCURRENCY,
                 queue_max_size: int = DEFAULT.QUEUE_MAX_SIZE,
                 logger: logging.Logger = None):
        """
        Simplified layer - creates nodes implicitly from run method.
        :param concurrency: number of concurrent nodes
        :param queue_max_size: max size of asyncio.Queue
        """
        self._nodes_auto_rerun = bool(nodes_auto_rerun)
        self._concurrency = int(concurrency)
        self._queue_max_size = int(queue_max_size)

        super().__init__(
            queue=asyncio.Queue(maxsize=self.queue_max_size),
            nodes=[
                Node(
                    do_auto_rerun=self.nodes_auto_rerun,
                    name=f'n{i + 1}',
                )
                for i in range(self.concurrency)
            ],
            logger=logger or logging.getLogger('aio_pipeline.layer.Layer'),
        )

    @property
    def nodes_auto_rerun(self) -> bool:
        return self._nodes_auto_rerun

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def queue_max_size(self) -> int:
        return self._queue_max_size

    @abc.abstractmethod
    async def node_run(self, node: Node) -> None:
        """
        Put item processing logic here.
        :param node: node.Node which will all this method
        :return: None
        """
        pass


class SoloLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = False


class HeadLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class MiddleLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class TailLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = False
