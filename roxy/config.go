package roxy

import (
	"github.com/laurent22/toml-go/toml"
	"path"
	"runtime"
)

var Configuration *Config

type Config struct {
	Doc toml.Document
}

func ParseConfig() *Config {
	if Configuration != nil {
		return Configuration
	}
	var parser toml.Parser
	_, filename, _, _ := runtime.Caller(1)
	filepath := path.Join(path.Dir(filename), "config.toml")
	Configuration = &Config{Doc: parser.ParseFile(filepath)}
	return Configuration
}
