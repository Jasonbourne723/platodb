package network

import (
	"fmt"
	"log"
	"strings"

	"github.com/Jasonbourne723/platodb/internal/database"
)

const (
	requeiredPass = "123"
)

type commandHandler func(args []string, session *Session) string

type commandProcessor struct {
	db       *database.DB
	commands map[string]commandHandler
}

func NewCommandProcessor() *commandProcessor {

	db, err := database.NewDB()
	if err != nil {
		log.Fatal(err)
	}

	processer := &commandProcessor{
		db:       db,
		commands: make(map[string]commandHandler),
	}

	processer.RegiseterCommand("auth", processer.authCommand)
	processer.RegiseterCommand("ping", processer.pingCommand)
	processer.RegiseterCommand("get", processer.getCommand)
	processer.RegiseterCommand("set", processer.setCommand)

	return processer
}

func (processer *commandProcessor) RegiseterCommand(command string, handler commandHandler) {
	processer.commands[strings.ToUpper(command)] = handler
}

func (processer *commandProcessor) pingCommand(args []string, session *Session) string {
	return "+PONG\r\n"
}

func (processer *commandProcessor) authCommand(args []string, session *Session) string {
	if args[0] == requeiredPass {
		session.authenticated = true
		return "+OK\r\n"
	}
	return "-ERR Authication failed"
}

func (processer *commandProcessor) setCommand(args []string, session *Session) string {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'SET' command\r\n"
	}
	processer.db.Set(args[0], []byte(args[1]))
	return "+OK\r\n"
}

func (processer *commandProcessor) getCommand(args []string, session *Session) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	value, err := processer.db.Get(args[0])
	if err != nil {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func (processor *commandProcessor) flush() {

	processor.db.Shutdown()
}
