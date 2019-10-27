import asyncio

from aio_pipeline import *


class CounterNode(Node):

    async def run(self, layer):
        print(self)
        x = 0

        for i in range(8):
            x += i
            await asyncio.sleep(0.2)
            print(x)
        else:
            layer.abort()


class WaiterNode(Node):

    async def run(self, layer):
        print(self)
        b = True

        while True:
            if b:
                print('tik')
            else:
                print('tok')

            await asyncio.sleep(0.1)
            b = not b


async def main():
    l = BaseLayer(
        nodes=[CounterNode('counter'), WaiterNode('waiter')],
    )
    p = Pipeline([l, ])

    await p.start()


if __name__ == '__main__':
    asyncio.run(main())
