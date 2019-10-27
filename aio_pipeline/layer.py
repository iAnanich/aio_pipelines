import abc
import typing
import asyncio

from .node import Node
from .event import OwnedEvent
from .state import STATES, State, WrongState

__all__ = (
    'Layer', 'HeadLayer', 'MiddleLayer', 'TailLayer',
)


class AbstractLayer(metaclass=abc.ABCMeta):
    class DEFAULT:
        QUEUE_MAX_SIZE: int
        CONCURRENCY: int
        SINGLE_NODE_STOP: bool

    needs_next_layer: bool

    def __init__(self, queue_max_size: int,
                 concurrency: int,
                 single_node_stop: bool):
        """
        :param queue_max_size: max size of the asyncio.Queue which is going to
        be the main source of items for this Layer.
        :param concurrency: number of concurrent Nodes.
        :param single_node_stop: if True, layer will start stopping itself after
        single node completed its work.
        """
        self.queue_max_size = queue_max_size
        self.concurrency = concurrency
        self.single_node_stop = single_node_stop

        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)

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
    async def stop_at_event(self, event: asyncio.Event, join_queue: bool = True) -> None:
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
    async def run(self):
        """
        Main logic here: process items using take_item, forward_item, done_item.
        This coroutine is going to be executed concurrently.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def before_start(self):
        """
        Executes before starting concurrent nodes.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def before_stop(self):
        """
        Executes before stopping concurrent nodes.
        :return:
        """
        pass


class BaseLayer(AbstractLayer, metaclass=abc.ABCMeta):
    class DEFAULT:
        QUEUE_MAX_SIZE = 0
        CONCURRENCY = 1
        SINGLE_NODE_STOP = False

    STATES = STATES

    needs_next_layer: bool = None

    def __init__(self, queue_max_size: int = DEFAULT.QUEUE_MAX_SIZE,
                 concurrency: int = DEFAULT.CONCURRENCY,
                 single_node_stop: bool = DEFAULT.SINGLE_NODE_STOP):
        """

        :param queue_max_size: max size of the asyncio.Queue which is going to
        be the main source of items for this Layer.
        :param concurrency: number of concurrent Nodes.
        :param single_node_stop: if True, layer will start stopping itself after
        single node completed its work.
        """
        super().__init__(
            queue_max_size=int(queue_max_size),
            concurrency=int(concurrency),
            single_node_stop=bool(single_node_stop),
        )

        self.state: State = self.STATES.IDLE
        self.started_event = OwnedEvent(owner=self, name='layer started')
        self.going_to_stop_event = OwnedEvent(owner=self, name='layer going to stop')
        self.stopped_event = OwnedEvent(owner=self, name='layer stopped')
        self.aborting_event = OwnedEvent(owner=self, name='layer aborted')

        self.running_task: asyncio.Task = None
        self.finalizer_task: asyncio.Task = None
        self.finalizer_lock = asyncio.Lock()

        self.concurrent_nodes: typing.Tuple[Node, ...] = None

        self.next_layer = None

    def connect_next_layer(self, next_layer: 'Layer') -> None:
        self.next_layer = next_layer

    async def start(self) -> None:
        if self.state != self.STATES.IDLE:
            raise WrongState

        if self.needs_next_layer and not self.next_layer:
            raise RuntimeError

        self.state = self.STATES.RUNNING
        self.started_event.set()

        await self.before_start()
        await self._start_runner_task()
        await self.stop(join_queue=False)

    async def stop(self, join_queue: bool = True) -> None:
        async with self.finalizer_lock:
            if self.state == self.STATES.STOPPED:
                return

            self.state = self.STATES.GOING_TO_STOP
            self.going_to_stop_event.set()

            if join_queue:
                await self.queue.join()
            await self.before_stop()
            self.finalizer_task = asyncio.create_task(self._finish_runner_task())
            await self.finalizer_task

            self.state = self.STATES.STOPPED
            self.stopped_event.set()

    async def _run_with_stop_hook(self) -> None:
        self.node = Node(self.run())
        if self.single_node_stop:
            asyncio.create_task(self.stop_at_event(self.node.event))
        await self.node.start()

    async def _start_runner_task(self) -> None:
        self.concurrent_nodes = tuple(
            Node(self.run()) for _ in range(self.concurrency)
        )

        async def gather_nodes():
            await asyncio.gather(
                *(node.start() for node in self.concurrent_nodes),
                return_exceptions=True,
            )

        asyncio.create_task(self.stop_at_event(
            event=self.aborting_event,
            join_queue=False,
        ))
        self.running_task = asyncio.create_task(gather_nodes())
        await self.running_task

    async def _finish_runner_task(self) -> None:
        for node in self.concurrent_nodes:
            await node.stop()

        await self.running_task

    async def stop_at_event(self, event: asyncio.Event, join_queue: bool = True) -> None:
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

    @abc.abstractmethod
    async def run(self):
        pass

    @abc.abstractmethod
    async def before_start(self):
        pass

    @abc.abstractmethod
    async def before_stop(self):
        pass


class Layer(BaseLayer, metaclass=abc.ABCMeta):

    async def before_start(self):
        pass

    async def before_stop(self):
        pass


class HeadLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class MiddleLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = True


class TailLayer(Layer, metaclass=abc.ABCMeta):
    needs_next_layer = False
