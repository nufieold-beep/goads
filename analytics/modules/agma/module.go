package agma

import (
	"github.com/mitchellh/mapstructure"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
	"github.com/prebid/prebid-server/v4/config"
)

// Builder builds the agma analytics module.
func Builder(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
	if deps.HTTPClient == nil || deps.Clock == nil {
		return nil, nil
	}

	var moduleConfig config.AgmaAnalytics
	if cfg != nil {
		if err := mapstructure.Decode(cfg, &moduleConfig); err != nil {
			return nil, err
		}
	}

	if !moduleConfig.Enabled {
		return nil, nil
	}

	return NewModule(deps.HTTPClient, moduleConfig, deps.Clock)
}