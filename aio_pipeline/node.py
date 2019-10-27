import asyncio
import typing

from .event import OwnedEvent


class Node:

    def __init__(self, coro: typing.Coroutine):
        self.coro = coro
        self.event = OwnedEvent(owner=self, name='node task completed')
        self.task: asyncio.Task = None

    async def start(self):
        self.task = asyncio.create_task(coro=self.coro)
        await self.task
        self.event.set()

    async def stop(self):
        self.task.cancel()

    @property
    def state(self):
        try:
            return self.task._state if self.task else 'IDLE'
        except Exception as exc:
            print(exc)

    def __repr__(self):
        return f'<Node [{self.state}] event=[{self.event.state_verbose}]>'
