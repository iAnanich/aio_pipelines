import abc
import asyncio

from .event import OwnedEvent

__all__ = (
    'Node',
)


class AbstractNode(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def start(self, layer) -> None:
        """
        Start.
        :param layer: layer.Layer that controls that Node
        :return: None
        """
        pass

    @abc.abstractmethod
    async def stop(self) -> None:
        """
        Stop.
        :return: None
        """
        pass

    @abc.abstractmethod
    async def run(self, layer) -> None:
        """
        Put actual item processing logic here.
        :param layer: layer.BaseLayer
        :return: None
        """
        pass


class Node(metaclass=abc.ABCMeta):

    def __init__(self, name: str):
        self.name = name
        self.event = OwnedEvent(owner=self, name='node task completed')
        self.task: asyncio.Task = None
        self.layer = None

    async def start(self, layer) -> None:
        self.layer = layer
        self.task = asyncio.create_task(
            coro=self.run(layer=layer),
        )
        await self.task
        self.event.set()

    async def stop(self) -> None:
        self.task.cancel()

    @abc.abstractmethod
    async def run(self, layer) -> None:
        pass

    @property
    def state(self) -> str:
        return self.task._state if self.task else 'IDLE'

    def __repr__(self):
        return f'<Node [{self.state}] event=[{self.event.state_verbose}] "{self.name}">'
