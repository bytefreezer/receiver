package services

import (
	"github.com/n0needt0/bytefreezer-receiver/config"
)

type Services struct {
	Config *config.Config
}

func NewServices(conf *config.Config) *Services {
	return &Services{
		Config: conf,
	}
}
