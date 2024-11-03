package network

import (
	"fmt"
	"log"
	"strings"

	"github.com/Jasonbourne723/platodb/internal/database"
)

const (
	requeiredPass = "leryirep"
)

type commandHandler func(args []string, session *Session) string

type commandProcesser struct {
	db       *database.DB
	commands map[string]commandHandler
}

func NewCommandProcesser() *commandProcesser {

	db, err := database.NewDB()
	if err != nil {
		log.Fatal(err)
	}

	processer := &commandProcesser{
		db:       db,
		commands: make(map[string]commandHandler),
	}

	processer.RegiseterCommand("auth", processer.authCommand)
	processer.RegiseterCommand("ping", processer.pingCommand)
	processer.RegiseterCommand("get", processer.getCommand)
	processer.RegiseterCommand("set", processer.setCommand)

	return processer
}

func (processer *commandProcesser) RegiseterCommand(command string, handler commandHandler) {
	processer.commands[strings.ToUpper(command)] = handler
}

func (processer *commandProcesser) pingCommand(args []string, session *Session) string {
	return "+PONG\r\n"
}

func (processer *commandProcesser) authCommand(args []string, session *Session) string {
	if args[0] == requeiredPass {
		session.authenticated = true
		return "+OK\r\n"
	}
	return "-ERR Authication failed"
}

func (processer *commandProcesser) setCommand(args []string, session *Session) string {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'SET' command\r\n"
	}
	processer.db.Set(args[0], []byte(args[1]))
	return "+OK\r\n"
}

func (processer *commandProcesser) getCommand(args []string, session *Session) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	value, err := processer.db.Get(args[0])
	if err != nil {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}
