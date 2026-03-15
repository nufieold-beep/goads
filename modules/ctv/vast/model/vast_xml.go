package model

import (
	"encoding/xml"
	"fmt"
)

type Vast struct {
	XMLName xml.Name `xml:"VAST"`
	Version string   `xml:"version,attr,omitempty"`
	Ads     []Ad     `xml:"Ad"`
}

type Ad struct {
	ID       string   `xml:"id,attr,omitempty"`
	Sequence int      `xml:"sequence,attr,omitempty"`
	InLine   *InLine  `xml:"InLine,omitempty"`
	Wrapper  *Wrapper `xml:"Wrapper,omitempty"`
	InnerXML string   `xml:",innerxml"`
}

type InLine struct {
	AdSystem    *AdSystem    `xml:"AdSystem,omitempty"`
	AdTitle     string       `xml:"AdTitle,omitempty"`
	Advertiser  string       `xml:"Advertiser,omitempty"`
	Description string       `xml:"Description,omitempty"`
	Error       string       `xml:"Error,omitempty"`
	Impressions []Impression `xml:"Impression,omitempty"`
	Pricing     *Pricing     `xml:"Pricing,omitempty"`
	Creatives   *Creatives   `xml:"Creatives,omitempty"`
	Extensions  *Extensions  `xml:"Extensions,omitempty"`
	InnerXML    string       `xml:",innerxml"`
}

type Wrapper struct {
	AdSystem     *AdSystem    `xml:"AdSystem,omitempty"`
	VASTAdTagURI string       `xml:"VASTAdTagURI,omitempty"`
	Error        string       `xml:"Error,omitempty"`
	Impressions  []Impression `xml:"Impression,omitempty"`
	Creatives    *Creatives   `xml:"Creatives,omitempty"`
	Extensions   *Extensions  `xml:"Extensions,omitempty"`
	InnerXML     string       `xml:",innerxml"`
}

type AdSystem struct {
	Version string `xml:"version,attr,omitempty"`
	Value   string `xml:",chardata"`
}

type Impression struct {
	ID    string `xml:"id,attr,omitempty"`
	Value string `xml:",cdata"`
}

type Pricing struct {
	Model    string `xml:"model,attr,omitempty"`
	Currency string `xml:"currency,attr,omitempty"`
	Value    string `xml:",chardata"`
}

type Creatives struct {
	Creative []Creative `xml:"Creative,omitempty"`
}

type Creative struct {
	ID            string         `xml:"id,attr,omitempty"`
	AdID          string         `xml:"adId,attr,omitempty"`
	Sequence      int            `xml:"sequence,attr,omitempty"`
	UniversalAdID *UniversalAdId `xml:"UniversalAdId,omitempty"`
	Linear        *Linear        `xml:"Linear,omitempty"`
	InnerXML      string         `xml:",innerxml"`
}

type UniversalAdId struct {
	IDRegistry string `xml:"idRegistry,attr,omitempty"`
	IDValue    string `xml:"idValue,attr,omitempty"`
	Value      string `xml:",chardata"`
}

type Linear struct {
	SkipOffset     string          `xml:"skipoffset,attr,omitempty"`
	Duration       string          `xml:"Duration,omitempty"`
	MediaFiles     *MediaFiles     `xml:"MediaFiles,omitempty"`
	VideoClicks    *VideoClicks    `xml:"VideoClicks,omitempty"`
	TrackingEvents *TrackingEvents `xml:"TrackingEvents,omitempty"`
	AdParameters   *AdParameters   `xml:"AdParameters,omitempty"`
	InnerXML       string          `xml:",innerxml"`
}

type MediaFiles struct {
	MediaFile []MediaFile `xml:"MediaFile,omitempty"`
}

type MediaFile struct {
	ID                  string `xml:"id,attr,omitempty"`
	Delivery            string `xml:"delivery,attr,omitempty"`
	Type                string `xml:"type,attr,omitempty"`
	Width               int    `xml:"width,attr,omitempty"`
	Height              int    `xml:"height,attr,omitempty"`
	Bitrate             int    `xml:"bitrate,attr,omitempty"`
	MinBitrate          int    `xml:"minBitrate,attr,omitempty"`
	MaxBitrate          int    `xml:"maxBitrate,attr,omitempty"`
	Scalable            bool   `xml:"scalable,attr,omitempty"`
	MaintainAspectRatio bool   `xml:"maintainAspectRatio,attr,omitempty"`
	Codec               string `xml:"codec,attr,omitempty"`
	Value               string `xml:",cdata"`
}

type VideoClicks struct {
	ClickThrough  *ClickThrough   `xml:"ClickThrough,omitempty"`
	ClickTracking []ClickTracking `xml:"ClickTracking,omitempty"`
	CustomClick   []CustomClick   `xml:"CustomClick,omitempty"`
}

type ClickThrough struct {
	ID    string `xml:"id,attr,omitempty"`
	Value string `xml:",cdata"`
}

type ClickTracking struct {
	ID    string `xml:"id,attr,omitempty"`
	Value string `xml:",cdata"`
}

type CustomClick struct {
	ID    string `xml:"id,attr,omitempty"`
	Value string `xml:",cdata"`
}

type TrackingEvents struct {
	Tracking []Tracking `xml:"Tracking,omitempty"`
}

type Tracking struct {
	Event  string `xml:"event,attr,omitempty"`
	Offset string `xml:"offset,attr,omitempty"`
	Value  string `xml:",cdata"`
}

type AdParameters struct {
	XMLEncoded bool   `xml:"xmlEncoded,attr,omitempty"`
	Value      string `xml:",cdata"`
}

type Extensions struct {
	Extension []ExtensionXML `xml:"Extension,omitempty"`
}

type ExtensionXML struct {
	Type     string `xml:"type,attr,omitempty"`
	InnerXML string `xml:",innerxml"`
}

func SecToHHMMSS(seconds int) string {
	if seconds < 0 {
		seconds = 0
	}
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, secs)
}

func BuildNoAdVast(version string) []byte {
	if version == "" {
		version = "3.0"
	}
	vast := Vast{Version: version, Ads: []Ad{}}
	output, err := xml.MarshalIndent(vast, "", "  ")
	if err != nil {
		return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><VAST version="%s"/>`, version))
	}
	return append([]byte(xml.Header), output...)
}

func BuildSkeletonInlineVast(version string) *Vast {
	if version == "" {
		version = "3.0"
	}
	return &Vast{
		Version: version,
		Ads: []Ad{{
			ID:       "1",
			Sequence: 1,
			InLine: &InLine{
				AdSystem: &AdSystem{Value: "PBS-CTV"},
				AdTitle:  "Ad",
				Creatives: &Creatives{Creative: []Creative{{
					ID:       "1",
					Sequence: 1,
					Linear:   &Linear{Duration: "00:00:00"},
				}}},
			},
		}},
	}
}

func BuildSkeletonInlineVastWithDuration(version string, durationSec int) *Vast {
	vast := BuildSkeletonInlineVast(version)
	if len(vast.Ads) > 0 && vast.Ads[0].InLine != nil && vast.Ads[0].InLine.Creatives != nil &&
		len(vast.Ads[0].InLine.Creatives.Creative) > 0 && vast.Ads[0].InLine.Creatives.Creative[0].Linear != nil {
		vast.Ads[0].InLine.Creatives.Creative[0].Linear.Duration = SecToHHMMSS(durationSec)
	}
	return vast
}

func (v *Vast) Marshal() ([]byte, error) {
	v.clearInnerXML()
	output, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), output...), nil
}

func (v *Vast) MarshalCompact() ([]byte, error) {
	v.clearInnerXML()
	output, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), output...), nil
}

func (v *Vast) clearInnerXML() {
	for i := range v.Ads {
		v.Ads[i].InnerXML = ""
		if v.Ads[i].InLine != nil {
			v.Ads[i].InLine.InnerXML = ""
			if v.Ads[i].InLine.Creatives != nil {
				for j := range v.Ads[i].InLine.Creatives.Creative {
					v.Ads[i].InLine.Creatives.Creative[j].InnerXML = ""
					if v.Ads[i].InLine.Creatives.Creative[j].Linear != nil {
						v.Ads[i].InLine.Creatives.Creative[j].Linear.InnerXML = ""
					}
				}
			}
		}
		if v.Ads[i].Wrapper != nil {
			v.Ads[i].Wrapper.InnerXML = ""
		}
	}
}

func Unmarshal(data []byte) (*Vast, error) {
	var vast Vast
	if err := xml.Unmarshal(data, &vast); err != nil {
		return nil, err
	}
	return &vast, nil
}
