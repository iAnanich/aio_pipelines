import abc
import asyncio
import logging

from .event import OwnedEvent

__all__ = (
    'AbstractNode', 'BaseNode', 'Node',
)

log = logging.getLogger(__name__)


class AbstractNode(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def start(self, layer) -> None:
        """
        Start.
        :param layer: layer.Layer that controls that node
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


class BaseNode(AbstractNode, metaclass=abc.ABCMeta):

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
        if self.task is None:
            raise RuntimeError('No task created yet.')
        if self.task.cancelled():
            return
        self.task.cancel()

    @abc.abstractmethod
    async def run(self, layer) -> None:
        pass

    @property
    def state(self) -> str:
        return self.task._state if self.task else 'IDLE'

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'[{self.state}] event=[{self.event.state_verbose}] ' \
               f'"{self.name}">'


class Node(BaseNode):

    def __init__(self, name: str,
                 do_auto_rerun: bool = False):
        super().__init__(
            name=name,
        )

        self._do_auto_rerun = bool(do_auto_rerun)

    @property
    def do_auto_rerun(self):
        return self._do_auto_rerun

    async def _run_from_layer(self, layer):
        """
        Run the method, implement inside layer.
        :param layer: layer.Layer
        :return:
        """
        await layer.node_run(node=self)

    async def run(self, layer) -> None:
        try:
            if self.do_auto_rerun:
                while True:
                    await self._run_from_layer(layer=layer)
            else:
                await self._run_from_layer(layer=layer)
        except Exception as exc:
            log.exception(f'Caught unexpected error during node running - {exc}')
            try:
                await layer.stop()
            except Exception as stop_exc:
                log.exception(f'Could not stop gracefully due to error - {stop_exc}')
            finally:
                raise exc
