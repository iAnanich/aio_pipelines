import asyncio
import typing

from .event import OwnedEvent
from .layer import Layer
from .state import STATES, State, WrongState

__all__ = (
    'Pipeline',
)


class Pipeline:

    def __init__(self, layers: typing.Sequence[Layer]):
        self.layers = tuple(layers)
        self._connect_layers()

        self.state: State = STATES.IDLE
        self.started_event = OwnedEvent(owner=self, name='pipeline started')
        self.going_to_stop_event = OwnedEvent(owner=self, name='pipeline going to stop')
        self.stopped_event = OwnedEvent(owner=self, name='pipeline stopped')

        self._running_layers_task: asyncio.Task = None
        self._finalizer_task: asyncio.Task = None
        self._finalizer_lock = asyncio.Lock()

    async def start(self):
        if self.state != STATES.IDLE:
            raise WrongState

        self.state = STATES.RUNNING
        self.started_event.set()

        await self._run_layers()
        await self.stop()

    async def stop(self):
        async with self._finalizer_lock:
            if self.state == STATES.STOPPED:
                return

            self.state = STATES.GOING_TO_STOP
            self.going_to_stop_event.set()

            self._finalizer_task = asyncio.create_task(self._stop_layers())
            await self._finalizer_task

            self.state = STATES.STOPPED
            self.stopped_event.set()

    async def stop_at_event(self, event: asyncio.Event):
        await event.wait()
        await self.stop()

    async def _run_layers(self):
        async def gather_layers():
            await asyncio.gather(
                *[
                    layer.start() for layer in self.layers
                ],
                return_exceptions=True,
            )

        # schedule tasks for chain-like stop of the layers
        for idx in range(1, len(self.layers)):
            layer = self.layers[idx - 1]
            layer_to_stop = self.layers[idx]
            coro = layer_to_stop.stop_at_event(event=layer.stopped_event)
            asyncio.create_task(coro)

        # schedule task for stopping Pipeline after last Layer stopped
        asyncio.create_task(
            self.stop_at_event(self.layers[-1].stopped_event)
        )

        self._running_layers_task = asyncio.create_task(gather_layers())
        await self._running_layers_task

    async def _stop_layers(self):
        for layer in self.layers:
            await layer.stop()
        await self._running_layers_task

    def _connect_layers(self):
        for idx in range(1, len(self.layers)):
            prev_layer = self.layers[idx - 1]
            next_layer = self.layers[idx]
            prev_layer.connect_next_layer(next_layer)
