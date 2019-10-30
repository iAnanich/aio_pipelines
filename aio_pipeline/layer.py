import abc
import asyncio
import typing

from .event import OwnedEvent
from .node import Node
from .state import STATES, State, WrongState

__all__ = (
    'BaseLayer', 'Layer', 'SoloLayer', 'HeadLayer', 'MiddleLayer', 'TailLayer',
)


class AbstractLayer(metaclass=abc.ABCMeta):
    class DEFAULT:
        QUEUE: asyncio.Queue

    needs_next_layer: bool

    def __init__(self, nodes: typing.Collection[Node], queue: asyncio.Queue):
        """
        :param queue: asyncio.Queue used for retrieving items.
        :param nodes: node.Node meant to be run concurrently.
        """
        self.queue = queue
        self.nodes = tuple(nodes)

        self.started_event = OwnedEvent(owner=self, name='layer started')
        self.going_to_stop_event = OwnedEvent(owner=self, name='layer going to stop')
        self.stopped_event = OwnedEvent(owner=self, name='layer stopped')
        self.aborting_event = OwnedEvent(owner=self, name='layer aborted')
        self.state: State = STATES.IDLE
        self.next_layer = None

    @abc.abstractmethod
    def connect_next_layer(self, next_layer: 'BaseLayer') -> None:
        """
        Bind's next_layer object to current object in order to use it's queue
        for forwarding items.
        :param next_layer: Layer object, which is standing next to the current
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
        Aborts running layer without awaiting on queue to fully process.
        :return: None
        """
        pass

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

    needs_next_layer: bool = None

    def __init__(self, nodes: typing.Collection[Node],
                 queue: asyncio.Queue = DEFAULT.QUEUE):
        super().__init__(
            nodes=nodes,
            queue=queue,
        )

        self._running_task: asyncio.Task = None
        self._finalizer_task: asyncio.Task = None
        self._finalizer_lock = asyncio.Lock()

    def connect_next_layer(self, next_layer: 'Layer') -> None:
        self.next_layer = next_layer

    async def start(self) -> None:
        if self.state != STATES.IDLE:
            raise WrongState

        if self.needs_next_layer and not self.next_layer:
            raise RuntimeError

        self.state = STATES.RUNNING
        self.started_event.set()

        await self.before_start()
        await self._start_runner_task()
        await self.stop(join_queue=False)

    async def stop(self, join_queue: bool = True) -> None:
        async with self._finalizer_lock:
            if self.state == STATES.STOPPED:
                return

            self.state = STATES.GOING_TO_STOP
            self.going_to_stop_event.set()

            if join_queue:
                await self.queue.join()
            await self.before_stopping()

            self._finalizer_task = asyncio.create_task(self._finish_runner_task())
            await self._finalizer_task

            await self.after_stopped()
            if not self._stop_at_event_task.done() and not self._stop_at_event_task.cancelled():
                self._stop_at_event_task.cancel()

            self.state = STATES.STOPPED
            self.stopped_event.set()

    async def _start_runner_task(self) -> None:
        async def gather_nodes():
            await asyncio.gather(
                *(
                    node.start(layer=self)
                    for node in self.nodes
                ),
                return_exceptions=True,
            )

        self._stop_at_event_task = asyncio.create_task(self.stop_at_event(
            event=self.aborting_event,
            join_queue=False,
        ))
        self._running_task = asyncio.create_task(gather_nodes())
        await self._running_task

    async def _finish_runner_task(self) -> None:
        for node in self.nodes:
            await node.stop()

        await self._running_task

    async def stop_at_event(self, event: asyncio.Event,
                            join_queue: bool = True) -> None:
        await event.wait()
        await self.stop(join_queue=join_queue)

    async def forward_item(self, obj: object) -> None:
        await self.next_layer.queue.put(obj)

    async def take_item(self) -> object:
        return await self.queue.get()

    def done_item(self) -> None:
        self.queue.task_done()

    def abort(self) -> None:
        self.aborting_event.set()

    def __repr__(self) -> str:
        return f'<Layer [{self.state}]>'

    async def before_start(self) -> None:
        pass

    async def before_stopping(self) -> None:
        pass

    async def after_stopped(self) -> None:
        pass


class Layer(BaseLayer, metaclass=abc.ABCMeta):
    class DEFAULT(BaseLayer.DEFAULT):
        CONCURRENCY = 1
        QUEUE_MAX_SIZE = 0

    def __init__(self, concurrency: int = DEFAULT.CONCURRENCY,
                 queue_max_size: int = DEFAULT.QUEUE_MAX_SIZE):
        """
        Simplified layer - creates nodes implicitly from run method.
        :param concurrency: number of concurrent nodes
        :param queue_max_size: max size of asyncio.Queue
        """
        self._concurrency = int(concurrency)
        self._queue_max_size = int(queue_max_size)

        self._node_class = type('ThatNode', (Node,), {'run': self._run})

        super().__init__(
            queue=asyncio.Queue(maxsize=self.queue_max_size),
            nodes=[
                self._node_class(name=f'n{i + 1}')
                for i in range(self.concurrency)
            ]
        )

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def queue_max_size(self) -> int:
        return self._queue_max_size

    @abc.abstractmethod
    async def run(self, node) -> None:
        """
        Put item processing logic here.
        :param node: node.Node
        :return: None
        """
        pass

    @staticmethod
    async def _run(node, layer):
        await layer.run(node=node)


class SoloLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = False


class HeadLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class MiddleLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class TailLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = False
