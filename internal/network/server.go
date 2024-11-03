package network

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

type Session struct {
	authenticated bool
}

type Server struct {
}

var processer = NewCommandProcesser()

func (c *Server) Listen() {

	listener, err := net.Listen("tcp", "127.0.0.1:6399")
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	defer listener.Close()
	fmt.Println("TCP server listening on port 6399")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("err: %v\n", err)
			continue
		}
		go HandleConnection(conn)
	}

}

func HandleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	session := &Session{authenticated: false} // 每个连接有独立的会话

	for {
		command, args, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			continue
		}

		if command != "AUTH" && !session.authenticated {
			conn.Write([]byte("-ERR not authenticated\r\n"))
			continue
		}

		if handler, ok := processer.commands[command]; ok {
			handler(args, session)
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

func parseRESP(reader *bufio.Reader) (string, []string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	if line[0] != '*' {
		return "", nil, fmt.Errorf("invalid protocol")
	}

	numArgs, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return "", nil, fmt.Errorf("invalid argument count")
	}

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		if line[0] != '$' {
			return "", nil, fmt.Errorf("invalid bulk string")
		}

		argLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return "", nil, fmt.Errorf("invalid argument length")
		}

		arg := make([]byte, argLen+2)
		if _, err := io.ReadFull(reader, arg); err != nil {
			return "", nil, err
		}
		args[i] = string(arg[:argLen])
	}

	return strings.ToUpper(args[0]), args[1:], nil
}
