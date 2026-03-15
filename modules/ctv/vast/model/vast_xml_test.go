package model

import (
	"strings"
	"testing"
)

func TestBuildNoAdVast_DefaultVersion(t *testing.T) {
	xml := string(BuildNoAdVast(""))
	if !strings.Contains(xml, `<VAST version="3.0"></VAST>`) {
		t.Fatalf("expected default 3.0 no-ad VAST, got %s", xml)
	}
}

func TestBuildSkeletonInlineVastWithDuration(t *testing.T) {
	vast := BuildSkeletonInlineVastWithDuration("4.0", 45)
	if vast == nil || len(vast.Ads) != 1 || vast.Ads[0].InLine == nil {
		t.Fatal("expected skeleton VAST with one inline ad")
	}
	creative := vast.Ads[0].InLine.Creatives.Creative[0]
	if creative.Linear == nil || creative.Linear.Duration != "00:00:45" {
		t.Fatalf("expected duration 00:00:45, got %+v", creative.Linear)
	}
}

func TestMarshalAndUnmarshalRoundTrip(t *testing.T) {
	input := BuildSkeletonInlineVast("4.0")
	data, err := input.Marshal()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	parsed, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if parsed.Version != "4.0" {
		t.Fatalf("expected version 4.0, got %s", parsed.Version)
	}
	if len(parsed.Ads) != 1 {
		t.Fatalf("expected 1 ad, got %d", len(parsed.Ads))
	}
}

func TestBuildSkeletonInlineVastDefaults(t *testing.T) {
	vast := BuildSkeletonInlineVast("")
	if vast.Version != "3.0" {
		t.Fatalf("expected default version 3.0, got %s", vast.Version)
	}
	if len(vast.Ads) != 1 || vast.Ads[0].InLine == nil {
		t.Fatal("expected a single inline ad in skeleton")
	}
	creative := vast.Ads[0].InLine.Creatives.Creative[0]
	if creative.Linear == nil || creative.Linear.Duration != "00:00:00" {
		t.Fatalf("expected default duration 00:00:00, got %+v", creative.Linear)
	}
}

func TestMarshalCompactIncludesXMLHeader(t *testing.T) {
	input := BuildSkeletonInlineVast("4.0")
	data, err := input.MarshalCompact()
	if err != nil {
		t.Fatalf("unexpected marshal compact error: %v", err)
	}
	if !strings.HasPrefix(string(data), `<?xml version="1.0" encoding="UTF-8"?>`) {
		t.Fatalf("expected XML header, got %s", string(data))
	}
}

func TestMarshalClearsInnerXMLToAvoidDuplicates(t *testing.T) {
	parsed, err := Unmarshal([]byte(`<VAST version="4.0"><Ad id="1"><InLine><AdTitle>Example</AdTitle><Creatives><Creative><Linear><Duration>00:00:15</Duration></Linear></Creative></Creatives></InLine></Ad></VAST>`))
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	data, err := parsed.Marshal()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	xml := string(data)
	if strings.Count(xml, "<AdTitle>Example</AdTitle>") != 1 {
		t.Fatalf("expected AdTitle once after re-marshal, got %s", xml)
	}
	if strings.Count(xml, "<Duration>00:00:15</Duration>") != 1 {
		t.Fatalf("expected Duration once after re-marshal, got %s", xml)
	}
	if strings.Count(xml, "<InLine>") != 1 {
		t.Fatalf("expected a single InLine node after re-marshal, got %s", xml)
	}
}
