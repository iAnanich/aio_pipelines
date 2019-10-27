import abc
import asyncio

from .event import OwnedEvent

__all__ = (
    'Node',
)


class Node(metaclass=abc.ABCMeta):

    def __init__(self, name: str):
        self.name = name
        self.event = OwnedEvent(owner=self, name='node task completed')
        self.task: asyncio.Task = None
        self.layer = None

    async def start(self, layer):
        self.layer = layer
        self.task = asyncio.create_task(
            coro=self.run(layer=layer),
        )
        await self.task
        self.event.set()

    async def stop(self):
        self.task.cancel()

    @abc.abstractmethod
    async def run(self, layer):
        pass

    @property
    def state(self):
        return self.task._state if self.task else 'IDLE'

    def __repr__(self):
        return f'<Node [{self.state}] event=[{self.event.state_verbose}] "{self.name}">'
