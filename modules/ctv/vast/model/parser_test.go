package model

import (
	"errors"
	"testing"
)

func TestParseVastAdmValidXML(t *testing.T) {
	parsed, err := ParseVastAdm(`<VAST version="4.0"><Ad><InLine><Creatives><Creative><Linear><Duration>00:00:15</Duration></Linear></Creative></Creatives></InLine></Ad></VAST>`)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if parsed == nil || parsed.Version != "4.0" {
		t.Fatalf("expected parsed VAST version 4.0, got %+v", parsed)
	}
}

func TestParseVastOrSkeletonFallsBackWhenAllowed(t *testing.T) {
	vast, warnings, err := ParseVastOrSkeleton("not-vast", ParserConfig{AllowSkeletonVast: true, VastVersionDefault: "4.0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vast == nil || vast.Version != "4.0" {
		t.Fatal("expected skeleton vast with version 4.0")
	}
	if len(warnings) == 0 {
		t.Fatal("expected fallback warning")
	}
}

func TestParseVastOrSkeletonReturnsErrorWhenDisabled(t *testing.T) {
	_, _, err := ParseVastOrSkeleton("not-vast", ParserConfig{})
	if !errors.Is(err, ErrNotVAST) {
		t.Fatalf("expected ErrNotVAST, got %v", err)
	}
}

func TestExtractFirstAdAndDuration(t *testing.T) {
	vast := &Vast{
		Version: "4.0",
		Ads: []Ad{{
			ID: "ad-1",
			InLine: &InLine{
				Creatives: &Creatives{Creative: []Creative{{
					Linear: &Linear{Duration: "00:00:30"},
				}}},
			},
		}},
	}

	ad := ExtractFirstAd(vast)
	if ad == nil || ad.ID != "ad-1" {
		t.Fatalf("expected first ad ad-1, got %+v", ad)
	}
	if duration := ExtractDuration(vast); duration != "00:00:30" {
		t.Fatalf("expected duration 00:00:30, got %s", duration)
	}
	if !IsInLineAd(ad) {
		t.Fatal("expected ad to be inline")
	}
	if IsWrapperAd(ad) {
		t.Fatal("expected ad not to be wrapper")
	}
}

func TestExtractDurationFromWrapper(t *testing.T) {
	vast := &Vast{
		Ads: []Ad{{
			Wrapper: &Wrapper{
				Creatives: &Creatives{Creative: []Creative{{
					Linear: &Linear{Duration: "00:01:00"},
				}}},
			},
		}},
	}

	if duration := ExtractDuration(vast); duration != "00:01:00" {
		t.Fatalf("expected wrapper duration 00:01:00, got %s", duration)
	}
	if !IsWrapperAd(&vast.Ads[0]) {
		t.Fatal("expected ad to be wrapper")
	}
}

func TestParseDurationToSeconds(t *testing.T) {
	tests := map[string]int{
		"":            0,
		"00:00:15":    15,
		"00:01:30":    90,
		"01:02:03":    3723,
		"00:00:15.25": 15,
		"bad":         0,
	}

	for input, expected := range tests {
		if actual := ParseDurationToSeconds(input); actual != expected {
			t.Fatalf("expected %q to parse as %d, got %d", input, expected, actual)
		}
	}
}

func TestParseVastFromBytes(t *testing.T) {
	parsed, err := ParseVastFromBytes([]byte(`<VAST version="3.0"></VAST>`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Version != "3.0" {
		t.Fatalf("expected version 3.0, got %s", parsed.Version)
	}
}
