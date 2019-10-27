import typing
import asyncio

from .node import Node
from .event import OwnedEvent


class Layer:
    class STATES:
        IDLE = 1
        RUNNING = 2
        GOING_TO_STOP = 3
        STOPPED = 4

    class DEFAULT:
        QUEUE_MAX_SIZE = 0
        CONCURRENCY = 1
        SINGLE_NODE_STOP = False

    needs_next_layer: bool = False
    next_layer_type: typing.Type['Layer'] or None = None

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
        self.next_layer = None

        self.queue_max_size = int(queue_max_size)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)

        self.state = self.STATES.IDLE
        self.started_event = OwnedEvent(owner=self, name='layer started')
        self.going_to_stop_event = OwnedEvent(owner=self, name='layer going to stop')
        self.stopped_event = OwnedEvent(owner=self, name='layer stopped')

        self.running_task: asyncio.Task = None
        self.finalizer_task: asyncio.Task = None
        self.finalizer_lock = asyncio.Lock()

        self.concurrency = int(concurrency)
        self.concurrent_nodes: typing.Tuple[Node, ...] = None
        self.single_node_stop = bool(single_node_stop)

    def connect_next_layer(self, next_layer: 'Layer'):
        if not isinstance(next_layer, self.next_layer_type or Layer):
            raise TypeError
        self.next_layer = next_layer

    async def start(self):
        if self.state != self.STATES.IDLE:
            raise RuntimeError  # TODO: exception

        self.state = self.STATES.RUNNING
        self.started_event.set()

        await self._start_runner_task()
        await self.stop()

    async def stop(self):
        async with self.finalizer_lock:
            if self.state == self.STATES.STOPPED:
                return

            self.state = self.STATES.GOING_TO_STOP
            self.going_to_stop_event.set()

            await self.queue.join()
            await self._before_stop()
            self.finalizer_task = asyncio.create_task(self._finish_runner_task())
            await self.finalizer_task

            self.state = self.STATES.STOPPED
            self.stopped_event.set()

    async def _run_with_stop_hook(self):
        self.node = Node(self._run())
        if self.single_node_stop:
            asyncio.create_task(self.stop_at_event(self.node.event))
        await self.node.start()

    async def _start_runner_task(self):
        self.concurrent_nodes = tuple(
            Node(self._run()) for _ in range(self.concurrency)
        )

        async def gather_nodes():
            await asyncio.gather(
                *(node.start() for node in self.concurrent_nodes),
                return_exceptions=True,
            )

        self.running_task = asyncio.create_task(gather_nodes())
        await self.running_task

    async def _finish_runner_task(self):
        for node in self.concurrent_nodes:
            await node.stop()

        await self.running_task

    async def stop_at_event(self, event: asyncio.Event):
        await event.wait()
        await self.stop()

    async def forward_item(self, obj):
        await self.next_layer.queue.put(obj)

    async def read_item(self):
        return await self.queue.get()

    def done_item(self):
        self.queue.task_done()

    async def _run(self):
        pass

    async def _before_stop(self):
        pass
