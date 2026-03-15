package build

import (
	"encoding/json"

	"github.com/benbjohnson/clock"
	"github.com/prebid/prebid-server/v4/analytics"
	"github.com/prebid/prebid-server/v4/analytics/analyticsdeps"
	"github.com/prebid/prebid-server/v4/analytics/clients"
	"github.com/mitchellh/mapstructure"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/openrtb_ext"
	"github.com/prebid/prebid-server/v4/ortb"
	"github.com/prebid/prebid-server/v4/privacy"
)

// New initializes all configured analytics modules using the registered builders.
func New(cfg map[string]interface{}) analytics.Runner {
	deps := analyticsdeps.Deps{
		HTTPClient: clients.GetDefaultHttpInstance(),
		Clock:      clock.New(),
	}

	return NewWithDeps(cfg, deps, Builders())
}

// Collection of all the correctly configured analytics modules - implements the PBSAnalyticsModule interface
type EnabledAnalytics map[string]analytics.Module

// NewWithDeps initializes analytics modules using injected dependencies and builders.
func NewWithDeps(cfg map[string]interface{}, deps analyticsdeps.Deps, builders AnalyticsModuleBuilders) analytics.Runner {
	modules := make(EnabledAnalytics)

	for moduleName, buildFn := range builders {
		module, err := buildFn(moduleConfig(cfg, moduleName), deps)
		if err != nil {
			logger.Errorf("Could not initialize analytics module %s: %v", moduleName, err)
			continue
		}
		if module != nil {
			modules[moduleName] = module
		}
	}

	return modules
}

func moduleConfig(cfg map[string]interface{}, moduleName string) map[string]interface{} {
	if cfg == nil {
		return nil
	}

	raw, ok := cfg[moduleName]
	if !ok || raw == nil {
		return nil
	}

	if configMap, ok := raw.(map[string]interface{}); ok {
		return configMap
	}

	if configMap, ok := raw.(map[interface{}]interface{}); ok {
		converted := make(map[string]interface{}, len(configMap))
		for key, value := range configMap {
			stringKey, ok := key.(string)
			if !ok {
				continue
			}
			converted[stringKey] = value
		}
		return converted
	}

	decoded := make(map[string]interface{})
	if err := mapstructure.Decode(raw, &decoded); err != nil {
		return nil
	}

	return decoded
}

func (ea EnabledAnalytics) LogAuctionObject(ao *analytics.AuctionObject, ac privacy.ActivityControl) {
	for name, module := range ea {
		if isAllowed, cloneBidderReq := evaluateActivities(ao.RequestWrapper, ac, name); isAllowed {
			if cloneBidderReq != nil {
				ao.RequestWrapper = cloneBidderReq
			}
			cloneReq := updateReqWrapperForAnalytics(ao.RequestWrapper, name, cloneBidderReq != nil)
			module.LogAuctionObject(ao)
			if cloneReq != nil {
				ao.RequestWrapper = cloneReq
			}
		}
	}
}

func (ea EnabledAnalytics) LogVideoObject(vo *analytics.VideoObject, ac privacy.ActivityControl) {
	for name, module := range ea {
		if isAllowed, cloneBidderReq := evaluateActivities(vo.RequestWrapper, ac, name); isAllowed {
			if cloneBidderReq != nil {
				vo.RequestWrapper = cloneBidderReq
			}
			cloneReq := updateReqWrapperForAnalytics(vo.RequestWrapper, name, cloneBidderReq != nil)
			module.LogVideoObject(vo)
			if cloneReq != nil {
				vo.RequestWrapper = cloneReq
			}
		}

	}
}

func (ea EnabledAnalytics) LogCookieSyncObject(cso *analytics.CookieSyncObject) {
	for _, module := range ea {
		module.LogCookieSyncObject(cso)
	}
}

func (ea EnabledAnalytics) LogSetUIDObject(so *analytics.SetUIDObject) {
	for _, module := range ea {
		module.LogSetUIDObject(so)
	}
}

func (ea EnabledAnalytics) LogAmpObject(ao *analytics.AmpObject, ac privacy.ActivityControl) {
	for name, module := range ea {
		if isAllowed, cloneBidderReq := evaluateActivities(ao.RequestWrapper, ac, name); isAllowed {
			if cloneBidderReq != nil {
				ao.RequestWrapper = cloneBidderReq
			}
			cloneReq := updateReqWrapperForAnalytics(ao.RequestWrapper, name, cloneBidderReq != nil)
			module.LogAmpObject(ao)
			if cloneReq != nil {
				ao.RequestWrapper = cloneReq
			}
		}
	}
}

func (ea EnabledAnalytics) LogNotificationEventObject(ne *analytics.NotificationEvent, ac privacy.ActivityControl) {
	for name, module := range ea {
		component := privacy.Component{Type: privacy.ComponentTypeAnalytics, Name: name}
		if ac.Allow(privacy.ActivityReportAnalytics, component, privacy.ActivityRequest{}) {
			module.LogNotificationEventObject(ne)
		}
	}
}

// Shutdown - correctly shutdown all analytics modules and wait for them to finish
func (ea EnabledAnalytics) Shutdown() {
	for _, module := range ea {
		module.Shutdown()
	}
}

func evaluateActivities(rw *openrtb_ext.RequestWrapper, ac privacy.ActivityControl, componentName string) (bool, *openrtb_ext.RequestWrapper) {
	// returned nil request wrapper means that request wrapper was not modified by activities and doesn't have to be changed in analytics object
	// it is needed in order to use one function for all analytics objects with RequestWrapper
	component := privacy.Component{Type: privacy.ComponentTypeAnalytics, Name: componentName}
	if !ac.Allow(privacy.ActivityReportAnalytics, component, privacy.ActivityRequest{}) {
		return false, nil
	}
	blockUserFPD := !ac.Allow(privacy.ActivityTransmitUserFPD, component, privacy.ActivityRequest{})
	blockPreciseGeo := !ac.Allow(privacy.ActivityTransmitPreciseGeo, component, privacy.ActivityRequest{})

	if !blockUserFPD && !blockPreciseGeo {
		return true, nil
	}

	cloneReq := &openrtb_ext.RequestWrapper{
		BidRequest: ortb.CloneBidRequestPartial(rw.BidRequest),
	}

	if blockUserFPD {
		privacy.ScrubUserFPD(cloneReq)
	}
	if blockPreciseGeo {
		ipConf := privacy.IPConf{IPV6: ac.IPv6Config, IPV4: ac.IPv4Config}
		privacy.ScrubGeoAndDeviceIP(cloneReq, ipConf)
	}

	cloneReq.RebuildRequest()
	return true, cloneReq
}

func updateReqWrapperForAnalytics(rw *openrtb_ext.RequestWrapper, adapterName string, isCloned bool) *openrtb_ext.RequestWrapper {
	if rw == nil {
		return nil
	}
	reqExt, _ := rw.GetRequestExt()
	reqExtPrebid := reqExt.GetPrebid()
	if reqExtPrebid == nil {
		return nil
	}

	var cloneReq *openrtb_ext.RequestWrapper
	if !isCloned {
		cloneReq = &openrtb_ext.RequestWrapper{BidRequest: ortb.CloneBidRequestPartial(rw.BidRequest)}
	} else {
		cloneReq = nil
	}

	if len(reqExtPrebid.Analytics) == 0 {
		return cloneReq
	}

	// Remove the entire analytics object if the adapter module is not present
	if _, ok := reqExtPrebid.Analytics[adapterName]; !ok {
		reqExtPrebid.Analytics = nil
	} else {
		reqExtPrebid.Analytics = updatePrebidAnalyticsMap(reqExtPrebid.Analytics, adapterName)
	}
	reqExt.SetPrebid(reqExtPrebid)
	rw.RebuildRequest()

	if cloneReq != nil {
		cloneReq.RebuildRequest()
	}

	return cloneReq
}

func updatePrebidAnalyticsMap(extPrebidAnalytics map[string]json.RawMessage, adapterName string) map[string]json.RawMessage {
	newMap := make(map[string]json.RawMessage)
	if val, ok := extPrebidAnalytics[adapterName]; ok {
		newMap[adapterName] = val
	}
	return newMap
}
