import asyncio

from aio_pipeline import SoloLayer, Pipeline


class ExampleSoloLayer(SoloLayer):

    async def run(self, node):
        await asyncio.sleep(0.1)

        for i in range(8):
            print(f'create {i}')
            await asyncio.sleep(0.1)


async def main():
    l = ExampleSoloLayer()

    p = Pipeline([l, ])

    await p.start()


if __name__ == '__main__':
    asyncio.run(main())
