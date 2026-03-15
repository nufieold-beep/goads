package vast

type CTVVastConfig struct {
	Enabled            *bool                 `json:"enabled,omitempty" mapstructure:"enabled"`
	Receiver           string                `json:"receiver,omitempty" mapstructure:"receiver"`
	DefaultCurrency    string                `json:"default_currency,omitempty" mapstructure:"default_currency"`
	VastVersionDefault string                `json:"vast_version_default,omitempty" mapstructure:"vast_version_default"`
	MaxAdsInPod        int                   `json:"max_ads_in_pod,omitempty" mapstructure:"max_ads_in_pod"`
	SelectionStrategy  string                `json:"selection_strategy,omitempty" mapstructure:"selection_strategy"`
	CollisionPolicy    string                `json:"collision_policy,omitempty" mapstructure:"collision_policy"`
	AllowSkeletonVast  *bool                 `json:"allow_skeleton_vast,omitempty" mapstructure:"allow_skeleton_vast"`
	Placement          *PlacementRulesConfig `json:"placement,omitempty" mapstructure:"placement"`
	Debug              *bool                 `json:"debug,omitempty" mapstructure:"debug"`
}

type PlacementRulesConfig struct {
	Pricing             *PricingRulesConfig    `json:"pricing,omitempty" mapstructure:"pricing"`
	Advertiser          *AdvertiserRulesConfig `json:"advertiser,omitempty" mapstructure:"advertiser"`
	Categories          *CategoryRulesConfig   `json:"categories,omitempty" mapstructure:"categories"`
	PricingPlacement    string                 `json:"pricing_placement,omitempty" mapstructure:"pricing_placement"`
	AdvertiserPlacement string                 `json:"advertiser_placement,omitempty" mapstructure:"advertiser_placement"`
	Debug               *bool                  `json:"debug,omitempty" mapstructure:"debug"`
}

type PricingRulesConfig struct {
	FloorCPM   *float64 `json:"floor_cpm,omitempty" mapstructure:"floor_cpm"`
	CeilingCPM *float64 `json:"ceiling_cpm,omitempty" mapstructure:"ceiling_cpm"`
	Currency   string   `json:"currency,omitempty" mapstructure:"currency"`
}

type AdvertiserRulesConfig struct {
	BlockedDomains []string `json:"blocked_domains,omitempty" mapstructure:"blocked_domains"`
	AllowedDomains []string `json:"allowed_domains,omitempty" mapstructure:"allowed_domains"`
}

type CategoryRulesConfig struct {
	BlockedCategories []string `json:"blocked_categories,omitempty" mapstructure:"blocked_categories"`
	AllowedCategories []string `json:"allowed_categories,omitempty" mapstructure:"allowed_categories"`
}

const (
	DefaultVastVersion       = "3.0"
	DefaultCurrency          = "USD"
	DefaultMaxAdsInPod       = 10
	DefaultCollisionPolicy   = string(CollisionReject)
	DefaultReceiver          = "GAM_SSU"
	DefaultSelectionStrategy = "max_revenue"

	PlacementVastPricing   = "VAST_PRICING"
	PlacementExtension     = "EXTENSION"
	PlacementAdvertiserTag = "ADVERTISER_TAG"
)

func MergeCTVVastConfig(host, account, profile *CTVVastConfig) CTVVastConfig {
	result := CTVVastConfig{}

	if host != nil {
		result = mergeIntoConfig(result, *host)
	}
	if account != nil {
		result = mergeIntoConfig(result, *account)
	}
	if profile != nil {
		result = mergeIntoConfig(result, *profile)
	}

	return result
}

func mergeIntoConfig(dst, src CTVVastConfig) CTVVastConfig {
	if src.Enabled != nil {
		dst.Enabled = src.Enabled
	}
	if src.Receiver != "" {
		dst.Receiver = src.Receiver
	}
	if src.DefaultCurrency != "" {
		dst.DefaultCurrency = src.DefaultCurrency
	}
	if src.VastVersionDefault != "" {
		dst.VastVersionDefault = src.VastVersionDefault
	}
	if src.MaxAdsInPod != 0 {
		dst.MaxAdsInPod = src.MaxAdsInPod
	}
	if src.SelectionStrategy != "" {
		dst.SelectionStrategy = src.SelectionStrategy
	}
	if src.CollisionPolicy != "" {
		dst.CollisionPolicy = src.CollisionPolicy
	}
	if src.AllowSkeletonVast != nil {
		dst.AllowSkeletonVast = src.AllowSkeletonVast
	}
	if src.Debug != nil {
		dst.Debug = src.Debug
	}
	if src.Placement != nil {
		if dst.Placement == nil {
			dst.Placement = &PlacementRulesConfig{}
		}
		dst.Placement = mergePlacementRules(dst.Placement, src.Placement)
	}

	return dst
}

func mergePlacementRules(dst, src *PlacementRulesConfig) *PlacementRulesConfig {
	if dst == nil {
		dst = &PlacementRulesConfig{}
	}
	if src == nil {
		return dst
	}

	if src.Debug != nil {
		dst.Debug = src.Debug
	}
	if src.PricingPlacement != "" {
		dst.PricingPlacement = src.PricingPlacement
	}
	if src.AdvertiserPlacement != "" {
		dst.AdvertiserPlacement = src.AdvertiserPlacement
	}
	if src.Pricing != nil {
		if dst.Pricing == nil {
			dst.Pricing = &PricingRulesConfig{}
		}
		dst.Pricing = mergePricingRules(dst.Pricing, src.Pricing)
	}
	if src.Advertiser != nil {
		if dst.Advertiser == nil {
			dst.Advertiser = &AdvertiserRulesConfig{}
		}
		dst.Advertiser = mergeAdvertiserRules(dst.Advertiser, src.Advertiser)
	}
	if src.Categories != nil {
		if dst.Categories == nil {
			dst.Categories = &CategoryRulesConfig{}
		}
		dst.Categories = mergeCategoryRules(dst.Categories, src.Categories)
	}

	return dst
}

func mergePricingRules(dst, src *PricingRulesConfig) *PricingRulesConfig {
	if src.FloorCPM != nil {
		dst.FloorCPM = src.FloorCPM
	}
	if src.CeilingCPM != nil {
		dst.CeilingCPM = src.CeilingCPM
	}
	if src.Currency != "" {
		dst.Currency = src.Currency
	}
	return dst
}

func mergeAdvertiserRules(dst, src *AdvertiserRulesConfig) *AdvertiserRulesConfig {
	if len(src.BlockedDomains) > 0 {
		dst.BlockedDomains = src.BlockedDomains
	}
	if len(src.AllowedDomains) > 0 {
		dst.AllowedDomains = src.AllowedDomains
	}
	return dst
}

func mergeCategoryRules(dst, src *CategoryRulesConfig) *CategoryRulesConfig {
	if len(src.BlockedCategories) > 0 {
		dst.BlockedCategories = src.BlockedCategories
	}
	if len(src.AllowedCategories) > 0 {
		dst.AllowedCategories = src.AllowedCategories
	}
	return dst
}

func (cfg CTVVastConfig) ReceiverConfig() ReceiverConfig {
	rc := ReceiverConfig{
		Receiver:           ReceiverType(stringOrDefault(cfg.Receiver, DefaultReceiver)),
		DefaultCurrency:    stringOrDefault(cfg.DefaultCurrency, DefaultCurrency),
		VastVersionDefault: stringOrDefault(cfg.VastVersionDefault, DefaultVastVersion),
		MaxAdsInPod:        intOrDefault(cfg.MaxAdsInPod, DefaultMaxAdsInPod),
		SelectionStrategy:  SelectionStrategy(stringOrDefault(cfg.SelectionStrategy, DefaultSelectionStrategy)),
		CollisionPolicy:    CollisionPolicy(stringOrDefault(cfg.CollisionPolicy, DefaultCollisionPolicy)),
		AllowSkeletonVast:  boolValue(cfg.AllowSkeletonVast),
		Debug:              boolValue(cfg.Debug),
	}
	pr := cfg.buildPlacementRules(rc.DefaultCurrency)
	rc.Placement = pr

	return rc
}

func (cfg CTVVastConfig) buildPlacementRules(defaultCurrency string) PlacementRules {
	pr := PlacementRules{}
	if cfg.Placement == nil {
		return pr
	}
	defaultCurrency = stringOrDefault(defaultCurrency, DefaultCurrency)
	pr.Debug = boolValue(cfg.Placement.Debug)
	pr.PricingPlacement = stringOrDefault(cfg.Placement.PricingPlacement, PlacementVastPricing)
	pr.AdvertiserPlacement = stringOrDefault(cfg.Placement.AdvertiserPlacement, PlacementAdvertiserTag)
	if cfg.Placement.Pricing != nil {
		pr.Pricing = PricingRules{Currency: cfg.Placement.Pricing.Currency}
		if cfg.Placement.Pricing.FloorCPM != nil {
			pr.Pricing.FloorCPM = *cfg.Placement.Pricing.FloorCPM
		}
		if cfg.Placement.Pricing.CeilingCPM != nil {
			pr.Pricing.CeilingCPM = *cfg.Placement.Pricing.CeilingCPM
		}
		pr.Pricing.Currency = stringOrDefault(pr.Pricing.Currency, defaultCurrency)
	}
	if cfg.Placement.Advertiser != nil {
		pr.Advertiser = AdvertiserRules{
			BlockedDomains: cfg.Placement.Advertiser.BlockedDomains,
			AllowedDomains: cfg.Placement.Advertiser.AllowedDomains,
		}
	}
	if cfg.Placement.Categories != nil {
		pr.Categories = CategoryRules{
			BlockedCategories: cfg.Placement.Categories.BlockedCategories,
			AllowedCategories: cfg.Placement.Categories.AllowedCategories,
		}
	}

	return pr
}

func (cfg CTVVastConfig) IsEnabled() bool {
	return cfg.Enabled != nil && *cfg.Enabled
}

func boolPtr(b bool) *bool {
	return &b
}

func float64Ptr(f float64) *float64 {
	return &f
}

func stringOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func intOrDefault(value, fallback int) int {
	if value != 0 {
		return value
	}
	return fallback
}

func boolValue(value *bool) bool {
	if value != nil {
		return *value
	}
	return false
}
