import sys, os
import asyncio, logging
import aiohttp, time, json, re

API_KEY = "AIzaSyBjMpntUdBow1BjlvIxKwAw0iQTIPj7Kxo"
LOCAL = '127.0.0.1'

PORTS = {"Bailey": 10000, "Bona": 10001, "Campbell": 10002, "Clark": 10003, "Jaquez": 10004}

NEIGHBORS = {
    "Clark": ["Jaquez", "Bona"],
    "Campbell": ["Bailey", "Bona", "Jaquez"],
    "Bona": ["Bailey", "Clark", "Campbell"],
    "Bailey": ["Campbell", "Bona"],
    "Jaquez": ["Clark", "Campbell"]
}

SERVER_NAME = sys.argv[1]

class ServerProtocol(asyncio.Protocol):
    HISTORY = dict() 

    def __init__(self, name, ip=LOCAL, port=10000, max_length=1e6):
        self.name = name
        self.ip = ip
        self.port = port
        self.max_length = int(max_length)

    async def message_handler(self, reader, writer):
        # handle received messages
        data = await reader.read(self.max_length)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print("{} received {} from {}".format(self.name, message, addr))

        parsed_message = ServerMessage(self.name)
        sendback_message = await parsed_message.parse(message) + "\n"

        if sendback_message == "\n":
            await self.flood(message)

        elif sendback_message.startswith("old"):
            print("Old message, no flood.")

        if sendback_message.startswith("AT"):
            print("{} send: {}".format(self.name, sendback_message[:100]))
            writer.write(sendback_message.encode())

            if (len(sendback_message.split()) == 6):
                await self.flood(sendback_message)
        await writer.drain()
        writer.close()

    async def flood(self, message):
        # flood message to neighboring servers
        async def send_to_server(server_name, message):
            try:
                reader, writer = await asyncio.open_connection(LOCAL, PORTS[neighbor])
                writer.write(message.encode())
                await writer.drain()
                print("Flooded {} with message {}".format(neighbor, message))
                writer.close()

            except ConnectionRefusedError:
                print("Connection refused to {}".format(neighbor))
                pass

        for neighbor in NEIGHBORS[self.name]:
            print("Attempting to flood {} with message {}".format(neighbor, message))
            await asyncio.gather(send_to_server(neighbor, message))

    async def run_forever(self):
        server = await asyncio.start_server(self.message_handler, self.ip, self.port)
        print(f'serving on {server.sockets[0].getsockname()}')

        async with server:
            await server.serve_forever()
        server.close()



class ServerMessage:
    HISTORY = dict() 

    def __init__(self, server_name="server"):
        self.server_name = server_name
        self.commands = ["WHATSAT", "IAMAT", "AT"]



    async def parse(self, message):
        # parse incoming messages
        command_table = {
            "IAMAT": self.handle_i_am_at,
            "WHATSAT": self.handle_whats_at,
            "AT": self.handle_at
        }

        message_list = [msg for msg in message.strip().split() if len(msg)]
        if not self.validate_message(message):
            return f"? {message}"
        
        cmd = command_table.get(message_list[0], None)
        return await cmd(*message_list[1:])

    async def handle_i_am_at(self, client_id, coordinates, timestamp):
        # IAMAT command
        msg = f"AT {self.server_name} +{time.time() - float(timestamp)} {client_id} {coordinates} {timestamp}"
        ServerMessage.HISTORY[client_id] = msg
        return msg

    async def handle_whats_at(self, client_id, radius, max_results):
        # WHATSAT command
        location = ServerMessage.HISTORY[client_id].split()[4]
        match = re.match(r'([+-]\d+\.\d+)([+-]\d+\.\d+)', location)
        lat, lon = match.groups()
        formatted_location = f"{lat}%2C{lon}"
        radius = int(radius)
        print("Attempting to retrieve places at location {}".format(formatted_location))
        url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={API_KEY}&location={formatted_location}&radius={radius * 1000}"
        print(url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                response = await resp.json()
                if int(max_results) < len(response["results"]):
                    print("Limiting results to {} places".format(max_results))
                    response["results"] = response["results"][:int(max_results)]
                google_api_feedback = json.dumps(response, indent=4)
                return ServerMessage.HISTORY[client_id] + "\n" + google_api_feedback + "\n" + "\n"

    async def handle_at(self, server_name, time_difference, client_id, coordinates, timestamp):
        # AT command
        msg = f"AT {self.server_name} {time_difference} {client_id} {coordinates} {timestamp}"

        if client_id in ServerMessage.HISTORY:
            print("Comparing timestamps: {} and {}".format(ServerMessage.HISTORY[client_id].split()[5], timestamp))
            if float(ServerMessage.HISTORY[client_id].split()[5]) >= float(timestamp):
                print("Received old propagation")
                return "old"
        ServerMessage.HISTORY[client_id] = msg
        return ""
    


    def validate_message(self, message):
        # validate message format
        split_message = message.split()
        if len(split_message) != 4:
            if len(split_message) != 6 or split_message[0] != "AT":
                return False
            
        if split_message[0] == "IAMAT":
            if not re.match(r'^[+-]\d{2}(?:\.\d+)?[+-]\d{3}(?:\.\d+)?$', split_message[2]):
                return False
            elif not re.match(r'^[0-9]{10}(?:\.[0-9]{1,9})?$', split_message[3]):
                return False
            
        elif split_message[0] == "WHATSAT":
            if not split_message[1] in self.HISTORY and split_message[2].isdigit() and split_message[3].isdigit():
                return False
            if int(split_message[2]) > 50 or int(split_message[3]) > 20:
                return False
            
        elif split_message[0] == "AT":
            return True
        
        else:
            return False
        
        return True
    


def main():
    logging.basicConfig(filename=f"server_{SERVER_NAME}.log", format='%(levelname)s: %(message)s', filemode='w+', level=logging.INFO)
    print(f"Server {SERVER_NAME} started")
    server = ServerProtocol(SERVER_NAME, LOCAL, PORTS[SERVER_NAME])

    try:
        asyncio.run(server.run_forever())
    except KeyboardInterrupt:
        pass



if __name__ == '__main__':
    main()

