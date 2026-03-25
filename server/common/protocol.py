import socket
from .utils import Bet


class ConnectionClosedError(Exception):
    pass


class ProtocolError(Exception):
    pass


class Protocol:
    MSG_TYPE_BET = 1
    MSG_TYPE_ACK = 2

    TYPE_BYTES = 2
    LENGTH_BYTES = 4

    PAYLOAD_DELIMITER = ","

    @staticmethod
    def recv_all(sock: socket, expected_length: int) -> bytes:
        """
        Helper to avoid short reads.
        It ensures that all bytes in the buf slice are read from the connection, returning an error if any read operation fails.
        """
        data = bytearray()
        while len(data) < expected_length:
            packet = sock.recv(expected_length - len(data))
            if not packet:
                raise ConnectionClosedError("Connection closed while receiving data")
            data.extend(packet)
        return data

    @staticmethod
    def receive_message(sock: socket) -> tuple[int, list[str]]:
        """
        Reads a message from the socket, returning a tuple of (msgtype, payload).
        """
        msgtype_header = Protocol.recv_all(sock, Protocol.TYPE_BYTES)
        msgtype = int.from_bytes(msgtype_header, byteorder="big", signed=False)
        if msgtype != Protocol.MSG_TYPE_BET:
            raise ProtocolError(f"Invalid message type: {msgtype}")

        length_header = Protocol.recv_all(sock, Protocol.LENGTH_BYTES)
        length = int.from_bytes(length_header, byteorder="big", signed=False)

        if length == 0:
            return msgtype, []

        payload = Protocol.recv_all(sock, length)
        data = payload.decode("utf-8")
        return msgtype, data.split(Protocol.PAYLOAD_DELIMITER)

    @staticmethod
    def send_message(sock: socket, msgtype: int, data: list[str]) -> None:
        """
        Construct and sends a message with the given type and data to the socket.
        """
        if msgtype != Protocol.MSG_TYPE_ACK:
            raise ProtocolError(f"Invalid message type: {msgtype}")

        payload = Protocol.PAYLOAD_DELIMITER.join(data).encode("utf-8")

        msg_type_header = msgtype.to_bytes(
            Protocol.TYPE_BYTES, byteorder="big", signed=False
        )

        length_header = len(payload).to_bytes(
            Protocol.LENGTH_BYTES, byteorder="big", signed=False
        )

        sock.sendall(msg_type_header + length_header)

        if payload:
            sock.sendall(payload)
