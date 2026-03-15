package model

import (
	"encoding/xml"
	"errors"
	"strings"
)

var ErrNotVAST = errors.New("input does not contain VAST XML")
var ErrVASTParseFailure = errors.New("failed to parse VAST XML")

func ParseVastAdm(adm string) (*Vast, error) {
	if !strings.Contains(adm, "<VAST") {
		return nil, ErrNotVAST
	}

	var vast Vast
	if err := xml.Unmarshal([]byte(adm), &vast); err != nil {
		return nil, errors.Join(ErrVASTParseFailure, err)
	}

	return &vast, nil
}

type ParserConfig struct {
	AllowSkeletonVast  bool
	VastVersionDefault string
}

func ParseVastOrSkeleton(adm string, cfg ParserConfig) (*Vast, []string, error) {
	var warnings []string
	vast, err := ParseVastAdm(adm)
	if err == nil {
		return vast, warnings, nil
	}
	if !cfg.AllowSkeletonVast {
		return nil, warnings, err
	}

	version := cfg.VastVersionDefault
	if version == "" {
		version = "3.0"
	}
	warnings = append(warnings, "VAST parse failed, using skeleton: "+err.Error())
	return BuildSkeletonInlineVast(version), warnings, nil
}

func ParseVastFromBytes(data []byte) (*Vast, error) {
	return ParseVastAdm(string(data))
}

func ExtractFirstAd(vast *Vast) *Ad {
	if vast == nil || len(vast.Ads) == 0 {
		return nil
	}
	return &vast.Ads[0]
}

func ExtractDuration(vast *Vast) string {
	if vast == nil || len(vast.Ads) == 0 {
		return ""
	}
	ad := vast.Ads[0]
	if ad.InLine != nil && ad.InLine.Creatives != nil {
		for _, creative := range ad.InLine.Creatives.Creative {
			if creative.Linear != nil && creative.Linear.Duration != "" {
				return creative.Linear.Duration
			}
		}
	}
	if ad.Wrapper != nil && ad.Wrapper.Creatives != nil {
		for _, creative := range ad.Wrapper.Creatives.Creative {
			if creative.Linear != nil && creative.Linear.Duration != "" {
				return creative.Linear.Duration
			}
		}
	}
	return ""
}

func ParseDurationToSeconds(duration string) int {
	if duration == "" {
		return 0
	}
	if idx := strings.Index(duration, "."); idx != -1 {
		duration = duration[:idx]
	}
	parts := strings.Split(duration, ":")
	if len(parts) != 3 {
		return 0
	}

	var hours, minutes, seconds int
	if _, err := parseIntFromString(parts[0], &hours); err != nil {
		return 0
	}
	if _, err := parseIntFromString(parts[1], &minutes); err != nil {
		return 0
	}
	if _, err := parseIntFromString(parts[2], &seconds); err != nil {
		return 0
	}

	return hours*3600 + minutes*60 + seconds
}

func parseIntFromString(s string, result *int) (bool, error) {
	s = strings.TrimSpace(s)
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return false, errors.New("invalid character in number")
		}
		n = n*10 + int(c-'0')
	}
	*result = n
	return true, nil
}

func IsInLineAd(ad *Ad) bool {
	return ad != nil && ad.InLine != nil
}

func IsWrapperAd(ad *Ad) bool {
	return ad != nil && ad.Wrapper != nil
}
