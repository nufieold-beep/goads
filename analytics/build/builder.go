package build

import (
	agma "github.com/prebid/prebid-server/v4/analytics/modules/agma"
	filelogger "github.com/prebid/prebid-server/v4/analytics/modules/filelogger"
	pubstack "github.com/prebid/prebid-server/v4/analytics/modules/pubstack"
)

func builders() AnalyticsModuleBuilders {
	return AnalyticsModuleBuilders{
		"agma":     agma.Builder,
		"file":     filelogger.Builder,
		"pubstack": pubstack.Builder,
	}
}
