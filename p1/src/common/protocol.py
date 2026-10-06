"""
Socket communication protocol for Water Management.
Frame format: <STX><REQUEST|ANSWER><ETX><LRC>
Control chars: ENQ, ACK, NACK, EOT, STX, ETX
LRC = XOR of all bytes in the message payload.
"""

# Control characters
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
EOT = b'\x04'
STX = b'\x02'
ETX = b'\x03'


def calculate_lrc(data: bytes) -> bytes:
    """LRC = XOR of every byte in the payload."""
    lrc = 0
    for b in data:
        lrc ^= b
    return bytes([lrc])


def build_frame(payload: str) -> bytes:
    """Build <STX><payload><ETX><LRC>."""
    payload_bytes = payload.encode('utf-8')
    lrc = calculate_lrc(payload_bytes)
    return STX + payload_bytes + ETX + lrc


def parse_frame(frame: bytes):
    """
    Validate and extract the payload from a frame.
    Returns (payload_str, ok: bool).
    """
    if not frame or frame[0:1] != STX:
        return None, False
    # find ETX position
    etx_pos = frame.find(ETX)
    if etx_pos < 0:
        return None, False
    payload_bytes = frame[1:etx_pos]
    received_lrc = frame[etx_pos + 1:etx_pos + 2]
    if not received_lrc:
        return None, False
    if calculate_lrc(payload_bytes) != received_lrc:
        return None, False
    return payload_bytes.decode('utf-8'), True


def read_frame(sock) -> bytes:
    """Read a full frame from the socket (until LRC byte after ETX)."""
    buf = b''
    # read STX
    while True:
        ch = sock.recv(1)
        if not ch:
            return b''
        if ch == STX:
            buf += ch
            break
        # if it's a control char (ACK/NACK/EOT), return it alone
        if ch in (ACK, NACK, EOT, ENQ):
            return ch
    # read until ETX
    while True:
        ch = sock.recv(1)
        if not ch:
            return b''
        buf += ch
        if ch == ETX:
            break
    # read LRC byte
    lrc = sock.recv(1)
    if not lrc:
        return b''
    buf += lrc
    return buf


def send_message(sock, payload: str) -> bool:
    """
    Send a framed message and wait for ACK/NACK.
    Returns True if ACK, False if NACK or error.
    """
    frame = build_frame(payload)
    sock.sendall(frame)
    response = sock.recv(1)
    return response == ACK


def build_request(operation: str, *fields) -> str:
    """REQUEST = CodigoOperacion#campo1#...#campo n"""
    parts = [operation] + [str(f) for f in fields]
    return '#'.join(parts)


def parse_request(payload: str):
    """Split a payload by '#'. Returns (operation, [fields])."""
    parts = payload.split('#')
    return parts[0], parts[1:]
