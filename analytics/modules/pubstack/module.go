package pubstack

import (
	"github.com/mitchellh/mapstructure"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
)

type Config struct {
	Enabled     bool   `mapstructure:"enabled" json:"enabled"`
	ScopeId     string `mapstructure:"scopeid" json:"scopeId"`
	IntakeURL   string `mapstructure:"endpoint" json:"endpoint"`
	ConfRefresh string `mapstructure:"configuration_refresh_delay" json:"configuration_refresh_delay"`
	Buffers     struct {
		EventCount int    `mapstructure:"count" json:"count"`
		BufferSize string `mapstructure:"size" json:"size"`
		Timeout    string `mapstructure:"timeout" json:"timeout"`
	} `mapstructure:"buffers" json:"buffers"`
}

// Builder builds the pubstack analytics module.
func Builder(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
	if deps.HTTPClient == nil || deps.Clock == nil {
		return nil, nil
	}

	var moduleConfig Config
	if cfg != nil {
		if err := mapstructure.Decode(cfg, &moduleConfig); err != nil {
			return nil, err
		}
	}

	if !moduleConfig.Enabled {
		return nil, nil
	}
	if moduleConfig.IntakeURL == "" || moduleConfig.ScopeId == "" {
		return nil, nil
	}

	return NewModule(
		deps.HTTPClient,
		moduleConfig.ScopeId,
		moduleConfig.IntakeURL,
		moduleConfig.ConfRefresh,
		moduleConfig.Buffers.EventCount,
		moduleConfig.Buffers.BufferSize,
		moduleConfig.Buffers.Timeout,
		deps.Clock,
	)
}