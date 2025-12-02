// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"github.com/bytefreezer/receiver/config"
)

type Services struct {
	Config *config.Config
}

func NewServices(conf *config.Config) *Services {
	return &Services{
		Config: conf,
	}
}
