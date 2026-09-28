import socket
from typing import Callable, List, Optional, Tuple

STX = b"\x02"
ETX = b"\x03"
EOT = b"\x04"
ENQ = b"\x05"
ACK = b"\x06"
NACK = b"\x15"

IO_TIMEOUT = 5.0 # segundos de espera por defecto a un ACK/NACK o a una respuesta
MAX_RETRIES = 3 # intentos de reenvio cuando el otro extremo contesta NACK


class ProtocolError(ConnectionError):


#mensajes
def pack_message(op_code: str, *fields) -> str:
    return "#".join([op_code, *[str(f) for f in fields]])


def unpack_message(message: str) -> Tuple[str, List[str]]:
    parts = message.split("#")
    return parts[0], parts[1:]


# nivel de trama
def _lrc(data: bytes) -> bytes:
    """XOR byte a byte de los datos, segun el enunciado."""
    lrc = 0
    for b in data:
        lrc ^= b
    return bytes([lrc])


def _send_frame(sock: socket.socket, message: str) -> None:
    data = message.encode("utf-8")
    if STX in data or ETX in data:
        raise ValueError("el mensaje no puede contener los bytes STX/ETX")
    sock.sendall(STX + data + ETX + _lrc(data))


def _recv_byte(sock: socket.socket, timeout: Optional[float]) -> bytes:
    """Lee exactamente un byte. timeout=None bloquea indefinidamente."""
    sock.settimeout(timeout)
    try:
        b = sock.recv(1)
    except socket.timeout:
        raise ProtocolError("timeout esperando datos del otro extremo") from None
    if not b:
        raise ProtocolError("el otro extremo cerro la conexion")
    return b


def _read_frame_body(sock: socket.socket, timeout: Optional[float]) -> Optional[str]:
    """Lee DATA + ETX + LRC (el STX ya se ha consumido).
    Devuelve el mensaje, o None si la trama llego corrupta (LRC incorrecto)."""
    data = bytearray()
    while True:
        b = _recv_byte(sock, timeout)
        if b == ETX:
            break
        data += b
    received_lrc = _recv_byte(sock, timeout)
    if received_lrc != _lrc(bytes(data)):
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


#lado iniciador
def open_session(sock: socket.socket, timeout: float = IO_TIMEOUT) -> None:
    """ENQ -> espera ACK."""
    sock.sendall(ENQ)
    reply = _recv_byte(sock, timeout)
    if reply == NACK:
        raise ProtocolError("el otro extremo rechazo abrir la sesion (NACK)")
    if reply != ACK:
        raise ProtocolError(f"se esperaba ACK/NACK tras el ENQ y llego {reply!r}")


def request(sock: socket.socket, message: str,
            timeout: float = IO_TIMEOUT, retries: int = MAX_RETRIES) -> str:
    """Envia una peticion y devuelve la respuesta.

    REQUEST -> ACK/NACK (si NACK, se reenvia) -> ANSWER -> ACK/NACK (si la
    respuesta llega corrupta, se contesta NACK y el otro extremo la reenvia)."""
    #enviar la peticion hasta que sea confirmada con ACK
    for _ in range(retries):
        _send_frame(sock, message)
        reply = _recv_byte(sock, timeout)
        if reply == ACK:
            break
        if reply != NACK:
            raise ProtocolError(f"se esperaba ACK/NACK y llego {reply!r}")
    else:
        raise ProtocolError("peticion rechazada (NACK) demasiadas veces")

    #recibir la respuesta y confirmarla
    for _ in range(retries):
        first = _recv_byte(sock, timeout)
        if first != STX:
            raise ProtocolError(f"se esperaba STX y llego {first!r}")
        answer = _read_frame_body(sock, timeout)
        if answer is not None:
            sock.sendall(ACK)
            return answer
        sock.sendall(NACK)
    raise ProtocolError("respuesta corrupta demasiadas veces")


def close_session(sock: socket.socket) -> None:
    """Envia EOT. Si la conexion ya esta rota no hace nada."""
    try:
        sock.sendall(EOT)
    except OSError:
        pass


#lado respondedor
def accept_session(sock: socket.socket, timeout: Optional[float] = None) -> None:
    """Espera ENQ y contesta ACK. Si llega otra cosa, contesta NACK."""
    first = _recv_byte(sock, timeout)
    if first != ENQ:
        try:
            sock.sendall(NACK)
        except OSError:
            pass
        raise ProtocolError(f"se esperaba ENQ y llego {first!r}")
    sock.sendall(ACK)


def serve_one(sock: socket.socket, handler: Callable[[str], str],
              idle_timeout: Optional[float] = None,
              timeout: float = IO_TIMEOUT, retries: int = MAX_RETRIES) -> bool:
    """Atiende una peticion de la sesion.

    Espera una trama (hasta idle_timeout; None = bloquea), la valida y contesta
    ACK/NACK, llama a handler(peticion) para obtener la respuesta, la envia y
    espera su ACK/NACK.

    Devuelve True si la sesion continua, o False si el iniciador envio EOT."""
    #recibir la peticion
    wait = idle_timeout
    for _ in range(retries):
        first = _recv_byte(sock, wait)
        if first == EOT:
            return False
        if first != STX:
            raise ProtocolError(f"se esperaba STX/EOT y llego {first!r}")
        message = _read_frame_body(sock, timeout)
        if message is not None:
            sock.sendall(ACK)
            break
        sock.sendall(NACK)
        wait = timeout   # el reenvio debe llegar enseguida
    else:
        raise ProtocolError("peticion corrupta demasiadas veces")

    # enviar la respuesta hasta que sea confirmada
    answer = handler(message)
    for _ in range(retries):
        _send_frame(sock, answer)
        reply = _recv_byte(sock, timeout)
        if reply == ACK:
            return True
        if reply != NACK:
            raise ProtocolError(f"se esperaba ACK/NACK y llego {reply!r}")
    raise ProtocolError("respuesta rechazada (NACK) demasiadas veces")