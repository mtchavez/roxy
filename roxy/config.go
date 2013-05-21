package roxy

import (
	"github.com/laurent22/toml-go/toml"
)

var Configuration *Config

type Config struct {
	Doc toml.Document
}

func ParseConfig(filepath string) *Config {
	if Configuration != nil {
		return Configuration
	}
	var parser toml.Parser
	Configuration = &Config{Doc: parser.ParseFile(filepath)}
	return Configuration
}
