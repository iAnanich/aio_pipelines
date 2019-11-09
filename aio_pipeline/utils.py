import asyncio


def try_cancel(task: asyncio.Task):
    if not task.done() and not task.cancelled():
        task.cancel()
