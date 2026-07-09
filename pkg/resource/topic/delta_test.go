// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package topic

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	svcapitypes "github.com/aws-controllers-k8s/kafka-controller/apis/v1alpha1"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// encodeConfigs base64-encodes a map of topic configuration properties the same
// way the MSK API (and a user's Spec.Configs) represents them.
func encodeConfigs(m map[string]interface{}) *string {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return aws.String(base64.StdEncoding.EncodeToString(b))
}

// topicWith returns a *resource whose Spec.Configs is set to the supplied
// (already-encoded) value. A nil argument leaves Configs unset.
func topicWith(configs *string) *resource {
	return &resource{
		ko: &svcapitypes.Topic{
			Spec: svcapitypes.TopicSpec{
				Configs: configs,
			},
		},
	}
}

// configsDiffers reports whether the delta between desired (a) and latest (b)
// includes Spec.Configs. This exercises the full generated newResourceDelta,
// which invokes customPostCompare (the delta_post_compare hook).
func configsDiffers(a, b *resource) bool {
	return newResourceDelta(a, b).DifferentAt("Spec.Configs")
}

// The regression this whole fix exists for: the user leaves Configs unset, but
// DescribeTopic returns the FULL effective configuration. A naive string
// compare would report a (phantom) difference here, triggering a spurious
// UpdateTopic. It must report NO difference.
func TestConfigsDelta_DesiredUnset_LatestFullDefaults(t *testing.T) {
	desired := topicWith(nil)
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms":       "604800000",
		"cleanup.policy":     "delete",
		"max.message.bytes":  "1048588",
		"min.insync.replica": "1",
	}))

	if configsDiffers(desired, latest) {
		t.Error("expected NO Configs delta when desired is unset (server defaults must be ignored)")
	}
}

// The user sets a subset of keys that already match the server's effective
// configuration. Even though latest carries many additional default keys, there
// must be no delta.
func TestConfigsDelta_DesiredSubsetMatches(t *testing.T) {
	desired := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "604800000",
	}))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms":      "604800000",
		"cleanup.policy":    "delete",
		"max.message.bytes": "1048588",
	}))

	if configsDiffers(desired, latest) {
		t.Error("expected NO Configs delta when every user-specified key matches the effective config")
	}
}

// A genuine user change: a key the user set differs from the server's effective
// value. This MUST produce a delta so the change is reconciled and not silently
// dropped.
func TestConfigsDelta_DesiredValueDiffers(t *testing.T) {
	desired := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "111111111",
	}))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms":   "604800000",
		"cleanup.policy": "delete",
	}))

	if !configsDiffers(desired, latest) {
		t.Error("expected a Configs delta when a user-specified key differs from the effective config")
	}
}

// The user specifies a key that is absent from the latest effective config
// (e.g. it has not been applied yet). This MUST produce a delta.
func TestConfigsDelta_DesiredKeyMissingFromLatest(t *testing.T) {
	desired := topicWith(encodeConfigs(map[string]interface{}{
		"cleanup.policy": "compact",
	}))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "604800000",
	}))

	if !configsDiffers(desired, latest) {
		t.Error("expected a Configs delta when a user-specified key is missing from the effective config")
	}
}

// Numeric vs string representations of the same value must compare equal, since
// a topic-config JSON value may be encoded either way.
func TestConfigsDelta_NumericStringEquivalence(t *testing.T) {
	desired := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": 604800000,
	}))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "604800000",
	}))

	if configsDiffers(desired, latest) {
		t.Error("expected NO Configs delta when the same value is encoded as number vs string")
	}
}

// An empty desired Configs (explicitly set but with no keys) manages nothing and
// must not produce a delta against a fully-populated effective config.
func TestConfigsDelta_DesiredEmpty(t *testing.T) {
	desired := topicWith(encodeConfigs(map[string]interface{}{}))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "604800000",
	}))

	if configsDiffers(desired, latest) {
		t.Error("expected NO Configs delta when desired Configs specifies no keys")
	}
}

// Malformed (non-base64) desired Configs must not panic and must be treated as
// "no keys to compare" rather than crashing the reconcile.
func TestConfigsDelta_MalformedDesiredDoesNotPanic(t *testing.T) {
	desired := topicWith(aws.String("not-valid-base64!!!"))
	latest := topicWith(encodeConfigs(map[string]interface{}{
		"retention.ms": "604800000",
	}))

	if configsDiffers(desired, latest) {
		t.Error("expected NO Configs delta when desired Configs cannot be decoded")
	}
}

// Sanity: fields other than Configs must still be compared normally. A changed
// PartitionCount must always surface as a delta (the generated comparison is
// untouched by our hook).
func TestPartitionCountDelta_StillDetected(t *testing.T) {
	a := &resource{ko: &svcapitypes.Topic{Spec: svcapitypes.TopicSpec{
		PartitionCount: aws.Int64(2),
	}}}
	b := &resource{ko: &svcapitypes.Topic{Spec: svcapitypes.TopicSpec{
		PartitionCount: aws.Int64(1),
	}}}

	if !newResourceDelta(a, b).DifferentAt("Spec.PartitionCount") {
		t.Error("expected a PartitionCount delta when the desired count differs")
	}
}
