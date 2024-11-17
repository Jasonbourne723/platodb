package network

import (
	"fmt"
	"strings"

	"github.com/Jasonbourne723/platodb/internal/database"
)

const (
	requeiredPass = "123"
)

type commandHandler func(args []string) string

type commandProcessor struct {
	db       *database.DB
	commands map[string]commandHandler
}

func NewCommandProcessor(db *database.DB) *commandProcessor {

	processer := &commandProcessor{
		db:       db,
		commands: make(map[string]commandHandler),
	}

	processer.RegiseterCommand("ping", processer.pingCommand)
	processer.RegiseterCommand("get", processer.getCommand)
	processer.RegiseterCommand("set", processer.setCommand)
	processer.RegiseterCommand("del", processer.delCommand)

	return processer
}

func (processer *commandProcessor) RegiseterCommand(command string, handler commandHandler) {
	processer.commands[strings.ToUpper(command)] = handler
}

func (processor *commandProcessor) flush() {
	processor.db.Shutdown()
}

func (processer *commandProcessor) pingCommand(args []string) string {
	return "+PONG\r\n"
}

func (processer *commandProcessor) setCommand(args []string) string {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'SET' command\r\n"
	}
	processer.db.Set(args[0], []byte(args[1]))
	return "+OK\r\n"
}

func (processer *commandProcessor) getCommand(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	value, err := processer.db.Get(args[0])
	if err != nil {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func (processer *commandProcessor) delCommand(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	err := processer.db.Del(args[0])
	if err != nil {
		return "-"
	}
	return "+OK\r\n"
}
