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

// NewServer creates and initializes a new Server instance with the provided context and commandProcessor.
// It also applies any additional configuration options passed as functional arguments.
// Returns a pointer to the created Server and an error if any occurred during setup.
func NewServer(ctx context.Context, processor *CommandProcessor, options ...Options) (*Server, error) {
	s := &Server{
		processor: processor,
		ctx:       ctx,
	}

	for _, option := range options {
		option(s)
	}
	return s, nil
}

// WithAddress sets the listening address for the Server instance.
func WithAddress(address string) Options {
	return func(s *Server) {
		s.address = address
	}
}

type Server struct {
	address   string
	processor *CommandProcessor
	listener  net.Listener
	ctx       context.Context
}

type Session struct {
	authenticated bool
}

// Listen starts the TCP server to accept incoming connections.
// It binds to the address provided in the Server's configuration and listens for incoming TCP connections.
// Accepted connections are handed off to HandleConnection method for further processing.
// If the server's context is cancelled, the listener will be closed and the function will return nil.
// In case of any other error during listening, it returns the specific error encountered.
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

// Shutdown closes the server's listener and waits for pending operations to finish within the given context.
// It shuts down the server by closing the listener, flushing pending commands,
// and responds to the caller when shutdown is complete or times out.
// Returns nil if shutdown succeeds, or an error if the operation times out or encounters an issue.
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

// HandleConnection handles a single client connection.
// It reads commands from the connection, processes them, and sends responses back.
// The function ensures that the client is authenticated before executing non-AUTH commands.
// It also manages the session state for each individual connection.
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

		if command == "AUTH" {
			if args[0] == requeiredPass {
				session.authenticated = true
				conn.Write([]byte("+OK\r\n"))
				continue
			}
			conn.Write([]byte("-ERR Authication failed"))
			continue
		}

		if handler, ok := s.processor.commands[command]; ok {
			rep := handler(args)
			conn.Write([]byte(rep))
			continue
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

// parseRESP reads and parses a RESP (REdis Serialization Protocol) message from the given reader.
// It returns the command name, a slice of arguments, and an error if any.
// The function assumes the stream starts with an array header followed by bulk strings.
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
