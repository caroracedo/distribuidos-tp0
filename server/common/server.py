import socket
import logging
import signal
from .protocol import Protocol, ConnectionClosedError
from .utils import Bet, store_bets


class Server:
    MSG_BET_STORED_SUCCESS = (
        "action: apuesta_almacenada | result: success | dni: {dni} | numero: {numero}"
    )

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        # Add signal handler for graceful shutdown and a flag to control the server loop
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """
        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again.
        The server loop can be stopped gracefully by sending a SIGTERM signal to the process.
        """
        try:
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    self.__handle_client_connection(client_sock)
                except OSError:
                    if not self._running:
                        break
                    raise
        finally:
            self._free_resources()

    def __handle_client_connection(self, client_sock: socket) -> None:
        """
        Handle communication with a connected client.
        The server receives a bet from the client, stores it and sends an ACK message back to the client.
        If the client closes the connection, the server logs the event and continues to accept new connections. Any other exceptions are logged as errors.
        Finally, the client socket is closed to free resources.
        """
        try:
            while self._running:
                _, data = Protocol.receive_message(client_sock)

                bet = Bet(*data.split(Protocol.PAYLOAD_DELIMITER))
                store_bets([bet])
                logging.info(
                    self.MSG_BET_STORED_SUCCESS.format(
                        dni=bet.document, numero=bet.number
                    )
                )

                Protocol.send_message(client_sock, Protocol.MSG_TYPE_ACK, "")
        except ConnectionClosedError:
            logging.info(
                "action: connection_closed | result: success | msg: Client closed the connection"
            )
        except Exception as e:
            logging.error(
                f"action: receive_bet_and_send_ack | result: fail | error: {e}"
            )
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c

    def _handle_sigterm(self, *args):
        """
        Handle SIGTERM signal to gracefully shutdown the server.
        """
        logging.info("action: shutdown | result: in_progress | msg: SIGTERM received")
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        logging.info("action: shutdown | result: success")

    def _free_resources(self):
        """
        Free server resources, such as closing the server socket.
        """
        if self._server_socket:
            self._server_socket.close()
