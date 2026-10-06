"""
WM_WS_M - Watering Station Monitor.
Usage: python monitor.py <engine_port> <central_ip> <central_port> <ws_id>
"""
import sys
import socket
import threading
import time
import os

sys.path.insert(0, '..')
from common import protocol as proto

ws_id = None
central_sock = None
leak_active = False
running = True


def connect_to_central(ip, port):
    """Handshake with Central: ENQ -> ACK -> REG -> REG_OK."""
    global central_sock
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    print(f"[WS_M:{ws_id}] Connected to Central {ip}:{port}")

    # ENQ
    s.sendall(proto.ENQ)
    resp = s.recv(1)
    if resp != proto.ACK:
        print("[WS_M] Central did not ACK ENQ")
        sys.exit(1)

    # REG#ws_id#location
    location = f'Location-{ws_id}'
    payload = proto.build_request('REG', ws_id, location)
    s.sendall(proto.build_frame(payload))
    ack = s.recv(1)
    if ack != proto.ACK:
        print("[WS_M] REG not ACKed")
        sys.exit(1)

    # Read answer
    frame = proto.read_frame(s)
    answer, ok = proto.parse_frame(frame)
    if ok:
        print(f"[WS_M] Registration answer: {answer}")
    central_sock = s


def heartbeat_loop():
    """Send heartbeat to Central each second."""
    global running, leak_active
    while running:
        time.sleep(1)
        if central_sock is None:
            continue
        try:
            if leak_active:
                payload = proto.build_request('LEAK', ws_id)
            else:
                payload = proto.build_request('HEARTBEAT', ws_id)
            central_sock.sendall(proto.build_frame(payload))
            ack = central_sock.recv(1)
            if ack != proto.ACK:
                print("[WS_M] Heartbeat NACK")
            # read answer
            try:
                central_sock.settimeout(1.0)
                frame = proto.read_frame(central_sock)
                central_sock.settimeout(None)
            except socket.timeout:
                frame = b''
        except Exception as e:
            print(f"[WS_M] Heartbeat error: {e}")
            break


def monitor_engine(engine_port):
    """Connect to Engine and poll health."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((os.getenv('ENGINE_HOST', '127.0.0.1'), engine_port))
    # s.connect(('127.0.0.1', engine_port))
    print(f"[WS_M:{ws_id}] Connected to Engine on port {engine_port}")
    return s


def engine_heartbeat_loop(engine_sock):
    global leak_active
    while running:
        time.sleep(1)
        try:
            payload = proto.build_request('HEARTBEAT', ws_id)
            engine_sock.sendall(proto.build_frame(payload))
            ack = engine_sock.recv(1)
            if ack != proto.ACK:
                print("[WS_M] Engine heartbeat NACK")
                continue
            frame = proto.read_frame(engine_sock)
            if frame:
                ans, ok = proto.parse_frame(frame)
                if ok:
                    # ACK the answer
                    engine_sock.sendall(proto.ACK)
                    # print(f"[WS_M] Engine status: {ans}")
        except Exception as e:
            print(f"[WS_M] Engine heartbeat error: {e}")
            break


def input_listener(engine_sock):
    """Listen for user input to simulate a leak."""
    # global leak_active
    global leak_active, running
    print(f"\n[WS_M:{ws_id}] Press 'k' + ENTER to simulate a LEAK, 'r' to resolve.\n")
    while running:
        line = sys.stdin.readline().strip().lower()
        if line == 'k':
            leak_active = True
            # Notify engine
            payload = proto.build_request('KO', ws_id)
            try:
                engine_sock.sendall(proto.build_frame(payload))
                ack = engine_sock.recv(1)
                print(f"[WS_M:{ws_id}] *** Leak simulation triggered ***")
            except Exception as e:
                print(f"[WS_M] Error sending KO: {e}")
        elif line == 'r':
            leak_active = False
            try:
                payload = proto.build_request('LEAK_RESOLVED', ws_id)
                central_sock.sendall(proto.build_frame(payload))
                ack = central_sock.recv(1)
                print(f"[WS_M:{ws_id}] Leak resolved.")
            except Exception as e:
                print(f"[WS_M] Error resolving: {e}")
        elif line in ('q', 'quit', 'exit'):
            running = False
            break


def main():
    global ws_id, running
    if len(sys.argv) < 5:
        print("Usage: python monitor.py <engine_port> <central_ip> <central_port> <ws_id>")
        sys.exit(1)
    engine_port = int(sys.argv[1])
    central_ip = sys.argv[2]
    central_port = int(sys.argv[3])
    ws_id = sys.argv[4]

    connect_to_central(central_ip, central_port)
    engine_sock = monitor_engine(engine_port)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=engine_heartbeat_loop, args=(engine_sock,), daemon=True).start()

    input_listener(engine_sock)
    running = False
    print("[WS_M] Shutting down.")


if __name__ == '__main__':
    main()
