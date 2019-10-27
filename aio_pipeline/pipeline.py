import typing
import asyncio

from .layer import Layer


class Pipeline:

    def __init__(self, layers: typing.Sequence[Layer]):
        self.layers = tuple(layers)
        self._connect_layers()

        self.start_layers_future = self._create_start_future()
        self.stop_layers_future = self._create_stop_future()
        self.stop_self_task: asyncio.Task = None
        self.running_future: asyncio.Future = None

    async def start(self):
        self.stop_self_task = asyncio.create_task(
            self.stop_at_event(self.layers[-1].stopped_event)
        )
        self.running_future = asyncio.gather(
            self.start_layers_future,
            self.stop_layers_future,
            self.stop_self_task,
            return_exceptions=True,
        )
        await self.running_future
        await self.stop()

    async def stop(self):
        for layer in self.layers:
            await layer.stop()
        self.start_layers_future.cancel()

    async def stop_at_event(self, event: asyncio.Event):
        await event.wait()
        await self.stop()

    def _create_start_future(self) -> asyncio.Future:
        coros = [layer.start() for layer in self.layers]
        return asyncio.gather(*coros, return_exceptions=True,)

    def _create_stop_future(self) -> asyncio.Future:
        coros = []
        for idx in range(1, len(self.layers)):
            layer = self.layers[idx-1]
            layer_to_stop = self.layers[idx]
            coro = layer_to_stop.stop_at_event(event=layer.stopped_event)
            coros.append(coro)
        return asyncio.gather(*coros, return_exceptions=True,)

    def _connect_layers(self):
        for idx in range(1, len(self.layers)):
            prev_layer = self.layers[idx-1]
            next_layer = self.layers[idx]
            prev_layer.connect_next_layer(next_layer)
