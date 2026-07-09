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

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// The async-lifecycle guard that requeues UpdateTopic while the topic is not
// ACTIVE is generated declaratively from the resource's `updateable.when`
// config in generator.yaml (see GoCodeResourceIsUpdateable), so no hand-written
// helper is needed here.

// customPostCompare augments the generated delta comparison for the Topic
// resource.
//
// The Spec.Configs field is intentionally excluded from the generated string
// comparison (see `compare.is_ignored` in generator.yaml). DescribeTopic
// returns the FULL effective topic configuration - every Kafka property with
// its defaults merged in - whereas the user only supplies the subset of keys
// they wish to override. A direct string comparison of the two Base64 blobs
// therefore always differs, producing a perpetual phantom delta that triggers a
// spurious UpdateTopic (rejected by MSK as a terminal BadRequestException).
//
// To keep genuine Configs changes reconcilable, we perform a semantic,
// key-by-key comparison here: decode both Base64-encoded JSON documents and add
// a Spec.Configs delta only when a key the user explicitly set is missing from,
// or differs from, the server's effective configuration. Keys the user never
// specified are server-managed defaults and are ignored.
func customPostCompare(delta *ackcompare.Delta, a, b *resource) {
	if a == nil {
		return
	}

	desired := decodeTopicConfigs(a.ko.Spec.Configs)
	// A nil/empty desired Configs means the user is not managing any topic
	// configuration, so there is nothing to reconcile.
	if len(desired) == 0 {
		return
	}
	latest := decodeTopicConfigs(b.ko.Spec.Configs)

	for key, desiredVal := range desired {
		latestVal, ok := latest[key]
		if !ok || latestVal != desiredVal {
			delta.Add("Spec.Configs", a.ko.Spec.Configs, b.ko.Spec.Configs)
			return
		}
	}
}

// decodeTopicConfigs decodes a Base64-encoded JSON topic-configuration document
// into a map of property name to string value. It returns an empty map when the
// input is nil or cannot be decoded, so callers can treat malformed or absent
// configuration as "no keys to compare" rather than failing the reconcile.
func decodeTopicConfigs(encoded *string) map[string]string {
	out := map[string]string{}
	if encoded == nil || aws.ToString(encoded) == "" {
		return out
	}
	raw, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil {
		return out
	}
	// Topic configuration values may be encoded as strings or numbers in the
	// JSON document; decode into interface{} and normalise every value to its
	// string form for a stable comparison.
	generic := map[string]interface{}{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return out
	}
	for k, v := range generic {
		switch tv := v.(type) {
		case string:
			out[k] = tv
		default:
			// Marshal non-string scalars (numbers, bools) back to their JSON
			// literal so equal values compare equal regardless of source type.
			b, err := json.Marshal(tv)
			if err != nil {
				continue
			}
			out[k] = string(b)
		}
	}
	return out
}
