package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type Options func(s *Server)

func NewServer(ctx context.Context, processor *commandProcessor, options ...Options) (*Server, error) {
	s := &Server{
		processor: processor,
		ctx:       ctx,
	}

	for _, option := range options {
		option(s)
	}
	return s, nil
}

func WithAddress(address string) Options {
	return func(s *Server) {
		s.address = address
	}
}

type Server struct {
	address   string
	processor *commandProcessor
	listener  net.Listener
	ctx       context.Context
}

type Session struct {
	authenticated bool
}

func (s *Server) Listen() (err error) {

	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer s.listener.Close()
	fmt.Println("TCP server listening on port 6399")

	for {

		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				log.Println("Listener closed, stopping accept loop")
				return nil
			}
			fmt.Printf("err: %v\n", err)
			continue
		}
		go s.HandleConnection(conn)
	}

}

func (s *Server) Shutdown(ctx context.Context) error {

	if err := s.listener.Close(); err != nil {
		return err
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		s.processor.flush()
	}()

	select {
	case <-done:
		log.Println("Server shutdown completed successfully")
		return nil
	case <-ctx.Done():
		log.Println("Shutdown timed out")
		return ctx.Err()
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	session := &Session{authenticated: false} // 每个连接有独立的会话

	for {
		command, args, err := parseRESP(reader)
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				break
			}
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			continue
		}

		if command != "AUTH" && !session.authenticated {
			conn.Write([]byte("-ERR not authenticated\r\n"))
			continue
		}

		if handler, ok := s.processor.commands[command]; ok {
			rep := handler(args, session)
			conn.Write([]byte(rep))
			continue
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
