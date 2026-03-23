import socket
import logging
import signal
import threading
from .protocol import Protocol, ConnectionClosedError
from .utils import Bet, store_bets, load_bets, has_won


class Server:
    MSG_BATCH_RECEIVED_SUCCESS = (
        "action: apuesta_recibida | result: success | cantidad: {cantidad}"
    )
    MSG_BATCH_RECEIVED_FAIL = (
        "action: apuesta_recibida | result: fail | cantidad: {cantidad}"
    )
    MSG_LOTTERY_SUCCESS = "action: sorteo | result: success"

    def __init__(self, port, listen_backlog, total_agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        # Add signal handler for graceful shutdown and a flag to control the server loop
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        # Add attributes to track the state of the lottery
        self._winners_by_agency = {}
        self._total_agencies = total_agencies

        # Add concurrency primitives to handle multiple clients simultaneously
        self._lottery_barrier = threading.Barrier(
            total_agencies, action=self._run_lottery
        )
        self._file_lock = threading.Lock()
        self._client_threads = []

    def run(self):
        """
        Server that accepts new connections and handles each client in a separate thread.
        Multiple clients can be served concurrently. The server loop can be stopped gracefully by sending a SIGTERM signal to the process.
        All client threads are joined before shutdown.
        """
        try:
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    client_thread = threading.Thread(
                        target=self.__handle_client_connection, args=(client_sock,)
                    )
                    self._client_threads.append(client_thread)
                    client_thread.start()
                except OSError:
                    if not self._running:
                        break
                    raise
        finally:
            self._free_resources()

    def __handle_client_connection(self, client_sock: socket) -> None:
        """
        Handle communication with a connected client.
        The server receives messages from the client until the client closes the connection or an error occurs.
        Finally, the client socket is closed to free resources.
        """
        try:
            while self._running:
                msgtype, data = Protocol.receive_message(client_sock)

                if msgtype == Protocol.MSG_TYPE_BATCH:
                    self._handle_batch_and_send_ack(client_sock, data)

                elif msgtype == Protocol.MSG_TYPE_EOF:
                    self._handle_eof(data)

                elif msgtype == Protocol.MSG_TYPE_WINNERS_QUERY:
                    self._handle_winners_query(client_sock, data)
        except ConnectionClosedError:
            logging.info(
                "action: connection_closed | result: success | msg: Client closed the connection"
            )
        except Exception as e:
            self._lottery_barrier.abort()
            try:
                Protocol.send_message(client_sock, Protocol.MSG_TYPE_ERROR, [])
            finally:
                logging.error(self.MSG_BATCH_RECEIVED_FAIL.format(cantidad=len(data)))
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
        self._lottery_barrier.abort()
        logging.info("action: shutdown | result: success")

    def _free_resources(self):
        """
        Free server resources, such as closing the server socket.
        """
        if self._server_socket:
            self._server_socket.close()

        for client_thread in self._client_threads:
            client_thread.join()

    def _handle_batch_and_send_ack(self, client_sock: socket, data: list[list[str]]):
        """
        Handle a batch of bets received from a client by storing the bets and sending an acknowledgment.
        Thread-safe storage is ensured using a lock to prevent concurrent access conflicts.
        """
        batch = [Bet(*bet_line) for bet_line in data]
        with self._file_lock:
            store_bets(batch)
        logging.info(self.MSG_BATCH_RECEIVED_SUCCESS.format(cantidad=len(batch)))

        Protocol.send_message(client_sock, Protocol.MSG_TYPE_ACK, [])

    def _handle_eof(self, data: list[list[str]]):
        """
        Handle the end of a client's batch submission by waiting at the lottery barrier until all agencies have reached it.
        """
        self._lottery_barrier.wait()

    def _handle_winners_query(self, client_sock: socket, data: list[list[str]]):
        """
        Handle a query for the list of winners for a specific agency.
        Thread-safe access to shared state is ensured using a lock.
        """
        Protocol.send_message(
            client_sock,
            Protocol.MSG_TYPE_WINNERS,
            self._winners_by_agency[data[0][0]],
        )

    def _run_lottery(self):
        """
        Run the lottery by determining the winners for each agency based on the stored bets.
        Thread-safe access to the bets is ensured using a lock to prevent concurrent access conflicts.
        This function will be executed by one of the threads when all agencies have reached the barrier.
        """
        logging.info(self.MSG_LOTTERY_SUCCESS)

        with self._file_lock:
            for bet in load_bets():
                agency_id = str(bet.agency)
                if agency_id not in self._winners_by_agency:
                    self._winners_by_agency[agency_id] = []

                if has_won(bet):
                    self._winners_by_agency[agency_id] += [bet.document]
