"""
Protocolo de sockets <STX><DATA><ETX><LRC> con handshake ENQ/ACK y cierre EOT.
"""
import socket

STX = b"\x02"
ETX = b"\x03"
EOT = b"\x04"
ENQ = b"\x05"
ACK = b"\x06"
NAK = b"\x15"
MAX_RETRIES = 3


class ProtocolError(Exception):
    pass


class Closed(ProtocolError):
    pass


class ChannelTimeout(ProtocolError):
    pass


class EndOfTransmission(ProtocolError):
    pass


def lrc(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x


def build_frame(message: str) -> bytes:
    data = message.encode("utf-8")
    if STX in data or ETX in data:
        raise ValueError("El mensaje contiene bytes de control reservados")
    return STX + data + ETX + bytes([lrc(data)])


class Channel:
    """Envoltorio de un socket con buffer de lectura y timeouts"""

    def __init__(self, sock, timeout=None):
        self.sock = sock
        self.buf = bytearray()
        sock.settimeout(timeout)

    def set_timeout(self, timeout):
        self.sock.settimeout(timeout)

    def _fill(self):
        try:
            chunk = self.sock.recv(4096)
        except socket.timeout:
            raise ChannelTimeout("timeout esperando datos")
        except OSError as e:
            raise Closed(str(e))
        if not chunk:
            raise Closed("conexión cerrada por el otro extremo")
        self.buf.extend(chunk)

    def read_byte(self) -> bytes:
        while not self.buf:
            self._fill()
        return bytes([self.buf.pop(0)])

    def read_until(self, delim: bytes) -> bytes:
        while True:
            i = self.buf.find(delim)
            if i >= 0:
                data = bytes(self.buf[:i])
                del self.buf[:i + 1]
                return data
            self._fill()

    def send(self, raw: bytes):
        try:
            self.sock.sendall(raw)
        except OSError as e:
            raise Closed(str(e))

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def _read(ch):
    """Lee un frame completo o un byte de control. -> (kind, value, lrc_ok)"""
    b = ch.read_byte()
    if b == STX:
        data = ch.read_until(ETX)
        received_lrc = ch.read_byte()[0]
        return "frame", data.decode("utf-8", "replace"), received_lrc == lrc(data)
    return "ctrl", b, True


# ---------------------------------------------------------------- handshake
def client_handshake(ch):
    ch.send(ENQ)
    if ch.read_byte() != ACK:
        raise ProtocolError("El servidor rechazó el handshake (NACK)")


def accept_handshake(ch):
    if ch.read_byte() != ENQ:
        ch.send(NAK)
        raise ProtocolError("Se esperaba ENQ")
    ch.send(ACK)


def send_eot(ch):
    try:
        ch.send(EOT)
    except ProtocolError:
        pass


# ------------------------------------------------------------ petición/respuesta
def request(ch, message: str) -> str:
    """Envía una petición y devuelve la respuesta (con ACK/NACK y reintentos por LRC)."""
    for _ in range(MAX_RETRIES):
        ch.send(build_frame(message))
        resp = ch.read_byte()
        if resp == ACK:
            break
        if resp != NAK:
            raise ProtocolError(f"Respuesta de control inesperada: {resp!r}")
    else:
        raise ProtocolError("Petición no confirmada (NACK repetidos)")
    for _ in range(MAX_RETRIES):
        kind, value, ok = _read(ch)
        if kind == "frame" and ok:
            ch.send(ACK)
            return value
        ch.send(NAK)
    raise ProtocolError("Respuesta corrupta (LRC incorrecto repetido)")


def recv_request(ch) -> str:
    """Espera una petición, valida el LRC y responde ACK/NACK. EOT -> EndOfTransmission."""
    while True:
        kind, value, ok = _read(ch)
        if kind == "ctrl":
            if value == EOT:
                raise EndOfTransmission()
            ch.send(NAK)
            continue
        if ok:
            ch.send(ACK)
            return value
        ch.send(NAK)


def send_answer(ch, message: str):
    for _ in range(MAX_RETRIES):
        ch.send(build_frame(message))
        if ch.read_byte() == ACK:
            return
    raise ProtocolError("Respuesta no confirmada (NACK repetidos)")
