package filelogger

import (
	"github.com/mitchellh/mapstructure"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
)

// Config is the file logger module configuration.
// Empty Filename means the module is disabled.
type Config struct {
	Enabled  *bool  `mapstructure:"enabled" json:"enabled"`
	Filename string `mapstructure:"filename" json:"filename"`
}

// Builder builds the filelogger analytics module.
func Builder(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
	var moduleConfig Config
	if cfg != nil {
		if err := mapstructure.Decode(cfg, &moduleConfig); err != nil {
			return nil, err
		}
	}

	if moduleConfig.Filename == "" {
		return nil, nil
	}
	if moduleConfig.Enabled != nil && !*moduleConfig.Enabled {
		return nil, nil
	}

	return NewFileLogger(moduleConfig.Filename)
}