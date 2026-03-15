package build

import (
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
)

//go:generate go run ./generator/buildergen.go

// AnalyticsModuleBuilders maps analytics module names to their builder functions.
type AnalyticsModuleBuilders map[string]AnalyticsModuleBuilderFn

// AnalyticsModuleBuilderFn creates an analytics module from its config block and shared dependencies.
type AnalyticsModuleBuilderFn func(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error)

// Builders returns the registered analytics module builders.
func Builders() AnalyticsModuleBuilders {
	return builders()
}