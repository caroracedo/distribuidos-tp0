import socket
import logging
import signal


class Server:
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

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode("utf-8")
            addr = client_sock.getpeername()
            logging.info(
                f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
            )
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode("utf-8"))
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
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
