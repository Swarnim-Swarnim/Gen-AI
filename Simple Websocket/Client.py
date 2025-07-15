import websockets
import asyncio

async def ws_client():
    print("WebSocket: Client Connected.")
    url = "ws://127.0.0.1:7890"
    
    async with websockets.connect(url) as ws:
        while True:
            name = input("Your Name (type 'exit' to quit): ")
            if name.lower() == 'exit':
                print("Exiting client...")
                break

            age = input("Your Age: ")

            # Send values to the server
            await ws.send(name)
            await ws.send(age)

            # Receive and print server's response
            msg = await ws.recv()
            print(msg)

asyncio.run(ws_client())
