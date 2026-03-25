package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const CSVDelimiter = ","

const MsgWinnersQuerySuccess = "action: consulta_ganadores | result: success | cant_ganadores: %d"

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchMaxAmount int
	DataFile       string
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

// sendBatchAndReceive Sends a batch of data to the server and waits for a response. In case of failure, an error is returned.
func (c *Client) sendBatchAndReceive(batch [][]string) error {
	if err := SendMessage(c.conn, MsgTypeBatch, batch); err != nil {
		return err
	}

	responseType, _, err := ReceiveMessage(c.conn)
	if err != nil {
		return err
	}

	if responseType != MsgTypeAck && responseType != MsgTypeError {
		return fmt.Errorf("Unexpected message type")
	}

	if responseType == MsgTypeError {
		return fmt.Errorf("Server rejected batch")
	}

	return nil
}

// sendWinnersQueryAndReceive Sends a query to the server to check if the winners are ready. It waits for a response and returns true if the winners are ready, false otherwise. In case of failure, false is returned.
func (c *Client) sendWinnersQueryAndReceive() error {
	if err := SendMessage(c.conn, MsgTypeWinnersQuery, [][]string{{c.config.ID}}); err != nil {
		return err
	}

	msgType, data, err := ReceiveMessage(c.conn)
	if err != nil {
		return err
	}

	if msgType != MsgTypeWinners {
		return fmt.Errorf("Unexpected message type")
	}

	log.Infof(MsgWinnersQuerySuccess, len(data))
	return nil
}

// sendBatchStage Reads the data file line by line, creates batches of data, and sends them to the server. It also listens for a quit signal to gracefully shut down the client. In case of failure, error is returned.
func (c *Client) sendBatchStage(quit chan os.Signal) error {
	file, err := os.Open(c.config.DataFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var batch [][]string

	for scanner.Scan() {
		select {
		case <-quit:
			log.Infof("action: shutdown | result: in_progress | client_id: %v | msg: SIGTERM received", c.config.ID)
			log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
			return nil
		default:
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		lineWithAgency := append([]string{c.config.ID}, strings.Split(line, CSVDelimiter)...)
		batch = append(batch, lineWithAgency)

		if len(batch) >= c.config.BatchMaxAmount {
			if err := c.sendBatchAndReceive(batch); err != nil {
				return err
			}
			batch = nil
		}
	}

	if len(batch) > 0 {
		if err := c.sendBatchAndReceive(batch); err != nil {
			return err
		}
	}

	if err := SendMessage(c.conn, MsgTypeEOF, [][]string{{c.config.ID}}); err != nil {
		return err
	}

	return nil
}

// StartClientLoop Starts the client loop, which reads the data file and sends batches to the server.
func (c *Client) StartClientLoop(quit chan os.Signal) {
	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: create_client_socket | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer c.closeClientSocket()

	if err := c.sendBatchStage(quit); err != nil {
		log.Errorf("action: send_batch_stage | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if err := c.sendWinnersQueryAndReceive(); err != nil {
		log.Errorf("action: send_query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
}
