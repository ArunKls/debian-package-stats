import asyncio

async def countdown():
    for i in range(1, 11):
        print(i)
        await asyncio.sleep(1)

async def main():
    await countdown()

asyncio.run(main())