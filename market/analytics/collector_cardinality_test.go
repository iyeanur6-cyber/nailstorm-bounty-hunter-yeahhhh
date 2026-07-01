package analytics

import (
	"testing"
)

func TestCardinalityGuard(t *testing.T) {
	c := NewCollector().WithMaxTagCardinality(2)

	// Valid sample
	valid := MetricSample{Tags: []MetricTag{{Key: "k1", Value: "v1"}}}
	if !c.Record(valid) {
		t.Errorf("expected sample to be recorded")
	}

	// Invalid sample (exceeds limit of 2)
	invalid := MetricSample{Tags: []MetricTag{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
		{Key: "k3", Value: "v3"},
	}}
	if c.Record(invalid) {
		t.Errorf("expected sample to be dropped due to cardinality")
	}

	if c.Stats().DroppedCardinality != 1 {
		t.Errorf("expected 1 dropped cardinality metric, got %d", c.Stats().DroppedCardinality)
	}
}
