package model

type VastAd struct {
	ID             string
	AdSystem       string
	AdTitle        string
	Description    string
	Advertiser     string
	DurationSec    int
	ErrorURLs      []string
	ImpressionURLs []string
	Sequence       int
	RawVAST        []byte
}
