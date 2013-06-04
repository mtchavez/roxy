package roxy

import (
	"github.com/laurent22/toml-go/toml"
)

var Configuration *Config

// Config holds the Doc for the parsed toml config file
type Config struct {
	Doc toml.Document
}

// Uses github.com/laurent22/toml-go/toml package to parse
// a toml config file. Parses once and returns configuration
// if already parsed and set.
func ParseConfig(filepath string) *Config {
	if Configuration != nil {
		return Configuration
	}
	var parser toml.Parser
	Configuration = &Config{Doc: parser.ParseFile(filepath)}
	return Configuration
}
