import asyncio
import typing


class OwnedEvent(asyncio.Event):

    def __init__(self, owner, name: typing.Optional[str] = None,
                 loop: typing.Optional[asyncio.BaseEventLoop] = None):
        self._owner = owner
        self._name = name
        self._value: bool

        super().__init__(loop=loop)

    @property
    def owner(self):
        return self._owner

    @property
    def name(self) -> str or None:
        return self._name

    @property
    def is_set(self) -> bool:
        return self._value

    @property
    def state_verbose(self) -> str:
        return 'set' if self.is_set else 'unset'

    def __repr__(self):
        return f'<Event [{self.state_verbose}] owner={self.owner} name="{self.name}">'
