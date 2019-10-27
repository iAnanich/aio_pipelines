import asyncio

from aio_pipeline import Pipeline, HeadLayer, MiddleLayer, TailLayer


class ALayer(HeadLayer):

    async def run(self, node):
        print('a')
        await asyncio.sleep(0.1)

        for i in range(8):
            print(f'put {i}')
            await asyncio.sleep(0.1)

            await self.forward_item(i)


class BLayer(MiddleLayer):

    async def run(self, node):
        print(f'b {node.name}')
        await asyncio.sleep(0.1)

        while True:
            i = await self.take_item()
            await asyncio.sleep(0.2)

            if i % 3 == 0:
                continue
            else:
                print(f'b[{node.name}] pass {i}')

            await self.forward_item(i)
            self.done_item()


class CLayer(TailLayer):

    async def run(self, node):
        print(f'c')
        await asyncio.sleep(0.1)

        while True:
            i = await self.take_item()

            await asyncio.sleep(0.1)
            print(f'c[{node.name}] got {i}')

            self.done_item()


async def main():
    al = ALayer(concurrency=1)
    bl = BLayer(concurrency=2)
    cl = CLayer(concurrency=1)

    p = Pipeline([al, bl, cl])

    await p.start()


if __name__ == '__main__':
    asyncio.run(main())
