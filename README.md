
# AsyncIO Pipelines

* Python 3.7+

### Key concepts

Main idea was to create simple instrument for item processing where flow of each
item can be split up to discrete steps and every step could be run concurrently
to perform some IO.

* Pipeline - top-level object which connect Layers together and can start or stop them.
* Layer - helps items to flow through queues, controls concurrent Nodes.
* Node - handles items processing by interacting with Layer.

### Examples

The easiest way to start is to use `Layer` class - it is simplified to explicit
`Node`s creation:

```python
import asyncio
from aio_pipeline import Layer, Pipeline

class MyLayer(Layer):
    async def run(self, node) -> None:
        for i in range(5):
            await asyncio.sleep(1)
            print(node.name, i)
    
async def main():
    l = MyLayer(concurrency=2)
    p = Pipeline([l, ])
    await p.start()

if __name__ == '__main__':
    asyncio.run(main())
```

See more examples in `examples` folder.
