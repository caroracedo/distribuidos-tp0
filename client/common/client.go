package common

import (
	"net"
	"os"
	"strings"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const MsgBetSubmittedSuccess = "action: apuesta_enviada | result: success | dni: %s | numero: %s"

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	FirstName     string
	LastName      string
	Document      string
	Birthdate     string
	Number        string
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// createClientSocket Initializes client socket. It attempts to establish a connection to the server. If the connection fails, it will retry for a specified number of times (LoopAmount) with a delay between each attempt (LoopPeriod). If all attempts fail, it returns the last encountered error.
func (c *Client) createClientSocket() error {
	var err error

	for i := 0; i < c.config.LoopAmount; i++ {
		var conn net.Conn

		conn, err = net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn
			return nil
		}

		time.Sleep(c.config.LoopPeriod)
	}

	return err
}

// closeClientSocket Closes the client socket connection. If the connection is nil, it simply returns nil. Otherwise, it attempts to close the connection and returns any error that may occur during the process.
func (c *Client) closeClientSocket() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// sendBetAndReceiveAck Executes the client loop, which consists of sending a bet, receiving an acknowledgment, and logging the result. In case of any error during the process, it returns the error.
func (c *Client) sendBetAndReceiveAck() error {
	data := strings.Join([]string{
		c.config.ID,
		c.config.FirstName,
		c.config.LastName,
		c.config.Document,
		c.config.Birthdate,
		c.config.Number,
	}, PayloadDelimiter)
	if err := SendMessage(c.conn, MsgTypeBet, data); err != nil {
		return err
	}

	_, _, err := ReceiveMessage(c.conn)
	if err != nil {
		return err
	}

	log.Infof(MsgBetSubmittedSuccess, c.config.Document, c.config.Number)
	return nil
}

// StartClientLoop Starts the client loop, which consists of sending bets and receiving acknowledgments for a specified number of iterations (LoopAmount) with a delay between each iteration (LoopPeriod). The loop can be interrupted by receiving a signal on the quit channel, in which case it logs the shutdown process and exits gracefully.
func (c *Client) StartClientLoop(quit chan os.Signal) {
	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer c.closeClientSocket()

	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		if err := c.sendBetAndReceiveAck(); err != nil {
			log.Errorf("action: send_bet_and_receive_ack | result: fail | client_id: %v | msg: %v", c.config.ID, err)
			return
		}

		select {
		case <-time.After(c.config.LoopPeriod):
		case <-quit:
			log.Infof("action: shutdown | result: in_progress | client_id: %v | msg: SIGTERM received", c.config.ID)
			log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
			return
		}
	}
}
