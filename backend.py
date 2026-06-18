import asyncio
import json
from typing import Dict, List, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(
    title="Code-Realme-NonStop Sync Server",
    description="Real-time WebSocket state synchronization backend.",
    version="1.0.0"
)

# Enable CORS for frontend testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Central Server Database State
# Mimics the inMemoryDb structure on the client side
server_db: Dict[str, Dict[str, Any]] = {
    "profiles": {},
    "realmes": {},
    "activeCalls": {}
}

class ConnectionManager:
    """Manages active WebSocket connections and broadcasts messages."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"Client connected. Active channels: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"Client disconnected. Active channels: {len(self.active_connections)}")

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send message to a specific connection."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            print(f"Error sending direct message: {e}")

    async def broadcast(self, message: dict, exclude: WebSocket = None):
        """Broadcast message to all connected clients except optionally the sender."""
        bad_connections = []
        for connection in self.active_connections:
            if connection == exclude:
                continue
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"Failed to broadcast to channel: {e}")
                bad_connections.append(connection)
                
        # Clean up dead connections
        for dead_conn in bad_connections:
            self.disconnect(dead_conn)

manager = ConnectionManager()

@app.get("/")
def get_status():
    """Simple HTTP Status probe."""
    return {
        "status": "ONLINE",
        "active_peers": len(manager.active_connections),
        "synced_profiles": len(server_db["profiles"]),
        "synced_realmes": len(server_db["realmes"])
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Primary WebSocket channel routing all client sync payloads."""
    await manager.connect(websocket)
    try:
        while True:
            # Receive incoming string message and decode JSON
            data_str = await websocket.receive_text()
            try:
                payload = json.loads(data_str)
            except json.JSONDecodeError:
                print("Received non-JSON message stream.")
                continue

            msg_type = payload.get("type")
            print(f"Processing signal: {msg_type}")

            # 1. Full Sync request on connection boot
            if msg_type == 'REQUEST_FULL_SYNC':
                # Return the server-side state database directly to this client
                await manager.send_personal_message({
                    "type": "FULL_SYNC",
                    "db": server_db
                }, websocket)

            # 2. Complete State Update received
            elif msg_type == 'FULL_SYNC':
                client_db = payload.get("db", {})
                # Merge incoming profiles, realmes and active calls gracefully
                server_db["profiles"].update(client_db.get("profiles", {}))
                server_db["realmes"].update(client_db.get("realmes", {}))
                server_db["activeCalls"].update(client_db.get("activeCalls", {}))
                
                # Broadcast updated server-wide state to other peers
                await manager.broadcast({
                    "type": "FULL_SYNC",
                    "db": server_db
                }, exclude=websocket)

            # 3. Dedicated User Profile alignment
            elif msg_type == 'UPDATE_PROFILE':
                profile_data = payload.get("data", {})
                username = profile_data.get("username")
                if username:
                    server_db["profiles"][username] = profile_data
                    # Relay profile payload to all other connected sessions
                    await manager.broadcast({
                        "type": "UPDATE_PROFILE",
                        "data": profile_data
                    }, exclude=websocket)

            # 4. Group Organization / Realme alignment
            elif msg_type == 'UPDATE_REALME':
                realme_data = payload.get("data", {})
                realme_id = realme_data.get("id")
                if realme_id:
                    server_db["realmes"][realme_id] = realme_data
                    # Relay update to other sessions
                    await manager.broadcast({
                        "type": "UPDATE_REALME",
                        "data": realme_data
                    }, exclude=websocket)

            # 5. Real-time Telephony Comms Signal matching (WebRTC handshake signals)
            elif msg_type == 'UPDATE_CALL':
                call_data = payload.get("data", {})
                call_id = call_data.get("id")
                if call_id:
                    server_db["activeCalls"][call_id] = call_data
                    # Relay direct WebRTC routing events to the remote target client
                    await manager.broadcast({
                        "type": "UPDATE_CALL",
                        "data": call_data
                    }, exclude=websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"Telemetry socket connection error: {e}")
        manager.disconnect(websocket)

if __name__ == "__main__":
    # Start ASGI Server on Port 8080 (matching index.html's configured wsUrl)
    uvicorn.run("server:app", host="0.0.0.0", port=8080, reload=True)
