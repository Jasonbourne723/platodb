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

type CommandProcessor struct {
	db       *database.DB
	commands map[string]commandHandler
}

// NewCommandProcessor initializes and returns a new CommandProcessor instance.
// It sets up the initial command handlers for "ping", "get", "set", and "del" commands.
// The provided database instance is used to execute the corresponding database operations.
// Parameters:
// - db (*database.DB): The database connection instance to be used by the CommandProcessor.
// Returns:
// - *CommandProcessor: A newly configured CommandProcessor instance ready to process commands.
func NewCommandProcessor(db *database.DB) *CommandProcessor {

	processor := &CommandProcessor{
		db:       db,
		commands: make(map[string]commandHandler),
	}

	processor.RegisterCommand("ping", processor.pingCommand)
	processor.RegisterCommand("get", processor.getCommand)
	processor.RegisterCommand("set", processor.setCommand)
	processor.RegisterCommand("del", processor.delCommand)

	return processor
}

// RegisterCommand registers a command with its corresponding handler function in the CommandProcessor instance.
// The command string is converted to uppercase for case-insensitive handling.
// Parameters:
//
//	command (string): The command string to be registered.
//	handler (commandHandler): The function that handles the execution of the command.
func (processor *CommandProcessor) RegisterCommand(command string, handler commandHandler) {
	processor.commands[strings.ToUpper(command)] = handler
}

// flush shuts down the database connection associated with the CommandProcessor instance.
func (processor *CommandProcessor) flush() {
	processor.db.Shutdown()
}

// pingCommand responds to the "PING" command with a "+PONG\r\n" message, indicating service availability.
func (processor *CommandProcessor) pingCommand(args []string) string {
	return "+PONG\r\n"
}

// setCommand sets a key-value pair in the database if provided with exactly two arguments.
// Returns an error message if the number of arguments is incorrect or if the database operation fails.
// Otherwise, confirms successful operation.
func (processor *CommandProcessor) setCommand(args []string) string {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'SET' command\r\n"
	}
	err := processor.db.Set(args[0], []byte(args[1]))
	if err != nil {
		return "-ERR " + err.Error()
	}
	return "+OK\r\n"
}

// getCommand retrieves the value associated with a key from the database.
// It expects exactly one argument. If the number of arguments is incorrect,
// it returns an error message. If the key is not found in the database,
// it returns a special response indicating a nil value. Otherwise, it returns
// the value prefixed with its length in bytes.
func (processor *CommandProcessor) getCommand(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	value, err := processor.db.Get(args[0])
	if err != nil {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

// delCommand deletes a key from the database if provided with exactly one argument.
// Returns an error message if the number of arguments is incorrect or if the deletion fails.
// Otherwise, confirms successful operation with "+OK\r\n".
func (processor *CommandProcessor) delCommand(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}
	err := processor.db.Del(args[0])
	if err != nil {
		return "-ERR " + err.Error()
	}
	return "+OK\r\n"
}
