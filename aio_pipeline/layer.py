import typing
import asyncio


class Layer:
    class STATES:
        IDLE = 1
        RUNNING = 2
        GOING_TO_STOP = 3
        STOPPED = 4

    class DEFAULT:
        QUEUE_MAX_SIZE = 0

    needs_next_layer: bool = False
    next_layer_type: typing.Type['Layer'] or None = None

    def __init__(self, queue_max_size: int = DEFAULT.QUEUE_MAX_SIZE):
        self.next_layer = None
        self.queue_max_size = queue_max_size

        self.state = self.STATES.IDLE
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
        self.running_task: asyncio.Task = None

        self.started_event = asyncio.Event()
        self.stopping_event = asyncio.Event()
        self.stopped_event = asyncio.Event()

    def connect_next_layer(self, next_layer: 'Layer'):
        if not isinstance(next_layer, self.next_layer_type or Layer):
            raise TypeError
        self.next_layer = next_layer

    async def start(self):
        self.state = self.STATES.RUNNING
        self.started_event.set()

        self.running_task = asyncio.create_task(self._run())
        await self.running_task
        await self.stop()

    async def stop(self):
        self.state = self.STATES.GOING_TO_STOP
        self.stopping_event.set()

        await self.queue.join()
        await self._stop()

        self.running_task.cancel()

        self.state = self.STATES.STOPPED
        self.stopped_event.set()

    async def _run(self):
        pass

    async def _stop(self):
        pass

    async def stop_at_event(self, event: asyncio.Event):
        await event.wait()
        await self.stop()

    async def forward_item(self, obj):
        await self.next_layer.queue.put(obj)

    async def read_item(self):
        return await self.queue.get()

    def done_item(self):
        self.queue.task_done()

    def cancel(self):
        self.running_task.cancel()
