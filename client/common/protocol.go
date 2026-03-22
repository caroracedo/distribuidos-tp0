package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

const (
	MsgTypeBatch uint16 = 1
	MsgTypeAck   uint16 = 2
	MsgTypeError uint16 = 3

	TypeBytes   = 2
	LengthBytes = 4

	PayloadDelimiter = ","
	BatchDelimiter   = "\n"
)

// SendMessage Constructs and sends a message with the given type and data to the server. It returns an error if there was an issue sending the message.
func SendMessage(conn net.Conn, msgType uint16, data [][]string) error {
	if msgType != MsgTypeBatch {
		return fmt.Errorf("Invalid message type: %d", msgType)
	}

	parts := make([]string, len(data))
	for i, d := range data {
		parts[i] = strings.Join(d, PayloadDelimiter)
	}
	payload := []byte(strings.Join(parts, BatchDelimiter))

	msgTypeHeader := make([]byte, TypeBytes)
	binary.BigEndian.PutUint16(msgTypeHeader, msgType)

	lengthHeader := make([]byte, LengthBytes)
	binary.BigEndian.PutUint32(lengthHeader, uint32(len(payload)))

	if err := sendAll(conn, msgTypeHeader); err != nil {
		return err
	}

	if err := sendAll(conn, lengthHeader); err != nil {
		return err
	}

	if len(payload) > 0 {
		if err := sendAll(conn, payload); err != nil {
			return err
		}
	}

	return nil
}

// ReceiveMessage Reads a message from the server, returning the message type, payload, and any error encountered.
func ReceiveMessage(conn net.Conn) (uint16, []string, error) {
	msgTypeHeader := make([]byte, TypeBytes)
	if err := receiveAll(conn, msgTypeHeader); err != nil {
		return 0, nil, err
	}
	msgType := binary.BigEndian.Uint16(msgTypeHeader)
	if msgType != MsgTypeAck && msgType != MsgTypeError {
		return 0, nil, fmt.Errorf("Invalid message type: %d", msgType)
	}

	lengthHeader := make([]byte, LengthBytes)
	if err := receiveAll(conn, lengthHeader); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lengthHeader)

	if length == 0 {
		return msgType, []string{}, nil
	}

	payload := make([]byte, length)
	if err := receiveAll(conn, payload); err != nil {
		return 0, nil, err
	}

	return msgType, strings.Split(string(payload), PayloadDelimiter), nil
}

// sendAll Is a helper to avoid short writes. It ensures that all bytes in the data slice are sent over the connection, returning an error if any write operation fails.
func sendAll(conn net.Conn, data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		sent, err := conn.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += sent
	}
	return nil
}

// receiveAll Is a helper to avoid short reads. It ensures that all bytes in the buf slice are read from the connection, returning an error if any read operation fails.
func receiveAll(conn net.Conn, buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		read, err := conn.Read(buf[totalRead:])
		if err != nil {
			return err
		}
		totalRead += read
	}
	return nil
}
