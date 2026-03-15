package build

import (
	"errors"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
	"github.com/prebid/prebid-server/v4/privacy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubModule struct{}

func (stubModule) LogAuctionObject(*analytics.AuctionObject)                  {}
func (stubModule) LogVideoObject(*analytics.VideoObject)                      {}
func (stubModule) LogCookieSyncObject(*analytics.CookieSyncObject)            {}
func (stubModule) LogSetUIDObject(*analytics.SetUIDObject)                    {}
func (stubModule) LogAmpObject(*analytics.AmpObject)                          {}
func (stubModule) LogNotificationEventObject(*analytics.NotificationEvent)    {}
func (stubModule) Shutdown()                                                  {}

func TestNewWithDepsSkipsFailingBuilders(t *testing.T) {
	deps := analyticsdeps.Deps{Clock: clock.New()}
	built := false

	runner := NewWithDeps(map[string]interface{}{}, deps, AnalyticsModuleBuilders{
		"disabled": func(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
			return nil, nil
		},
		"error": func(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
			return nil, errors.New("boom")
		},
		"ok": func(cfg map[string]interface{}, deps analyticsdeps.Deps) (analytics.Module, error) {
			built = true
			return stubModule{}, nil
		},
	})

	enabled, ok := runner.(EnabledAnalytics)
	require.True(t, ok)
	assert.True(t, built)
	assert.Len(t, enabled, 1)
	assert.Contains(t, enabled, "ok")
}

func TestNewWithDepsReturnsRunnerForNilConfig(t *testing.T) {
	runner := NewWithDeps(nil, analyticsdeps.Deps{Clock: clock.New()}, AnalyticsModuleBuilders{})

	enabled, ok := runner.(EnabledAnalytics)
	require.True(t, ok)
	assert.Empty(t, enabled)
	assert.Implements(t, (*analytics.Runner)(nil), enabled)
	assert.NotPanics(t, func() {
		enabled.LogCookieSyncObject(nil)
		enabled.LogSetUIDObject(nil)
		enabled.LogNotificationEventObject(nil, privacy.ActivityControl{})
	})
}

func TestModuleConfigExtractsNestedMap(t *testing.T) {
	root := map[string]interface{}{
		"pubstack": map[string]interface{}{
			"enabled": true,
		},
	}

	assert.Equal(t, root["pubstack"], moduleConfig(root, "pubstack"))
}