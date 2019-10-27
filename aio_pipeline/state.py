
class WrongState(Exception):
    pass


class State:
    VALUES = (
        ('idle', 1),
        ('running', 2),
        ('going_to_stop', 3),
        ('stopped', 4),
    )

    def __init__(self, val: int):
        try:
            str_val = self.int_to_str(val)
        except KeyError:
            raise ValueError
        self._val = int(val)
        self._str_Val = str_val

    def __int__(self) -> int:
        return self._val
    val = int_val = int_value = value = property(__int__)

    def __str__(self) -> str:
        return self._str_Val
    str_val = str_value = verbose = property(__str__)

    def __repr__(self) -> str:
        return f'<State [{self.verbose}]>'

    @classmethod
    def int_to_str(cls, int_val: int) -> str:
        return {i: s for s, i in cls.VALUES}[int_val]

    @classmethod
    def str_to_int(cls, str_val: int) -> str:
        return {s: i for s, i in cls.VALUES}[str_val]


class STATES:
    IDLE = State(1)
    RUNNING = State(2)
    GOING_TO_STOP = State(3)
    STOPPED = State(4)
