package commands

import (
	"strings"
)

type CommandRegistry struct {
	commands map[string]func() RedisCommand
}

func NewCommandRegistry() *CommandRegistry {
	return &CommandRegistry{commands: make(map[string]func() RedisCommand)}
}

func (r *CommandRegistry) Register(cmd string) {
	switch strings.ToLower(cmd) {
	case "command":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &CommandCommand{} }
	case "ping":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &PingCommand{} }
	case "echo":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &EchoCommand{} }
	case "set":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &SetCommand{} }
	case "get":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &GetCommand{} }
	case "config":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &ConfigCommand{} }
	case "keys":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &KeysCommand{} }
	case "info":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &InfoCommand{} }
	case "replconf":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &ReplconfCommand{} }
	case "psync":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &PsyncCommand{} }
	case "wait":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &WaitCommand{} }
	case "type":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &TypeCommand{} }
	case "xadd":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &XaddCommand{} }
	case "xrange":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &XrangeCommand{} }
	case "xread":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &XreadCommand{} }
	case "incr":
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &IncrCommand{} }
	default:
		r.commands[strings.ToLower(cmd)] = func() RedisCommand { return &UnknownCommand{} }
	}
}

func (r *CommandRegistry) Get(cmd string) RedisCommand {
	factory, exists := r.commands[strings.ToLower(cmd)]
	if !exists {
		return &UnknownCommand{}
	}
	return factory()
}
