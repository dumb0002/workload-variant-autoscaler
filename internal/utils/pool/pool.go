/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/common"
)

type EndpointPool struct {
	Name           string
	Namespace      string
	Selector       map[string]string
	EndpointPicker EndpointPicker
}

type EndpointPicker struct {
	ServiceName       string
	Namespace         string
	MetricsPortNumber int32
}

func InferencePoolToEndpointPool(inferencePool *v1.InferencePool) (*EndpointPool, error) {
	if inferencePool == nil {
		return nil, nil
	}

	clientset, err := K8sClient()
	if err != nil {
		return nil, err
	}

	// Find EPP Metrics Port Number from EPP Service
	serviceName := string(inferencePool.Spec.EndpointPickerRef.Name)
	service, err := clientset.CoreV1().Services(inferencePool.Namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var portNumber int32
	existMetricPort := false

	for _, port := range service.Spec.Ports {
		if port.Name == "metrics" {
			portNumber = port.Port
			existMetricPort = true
			break
		}
	}

	if !existMetricPort {
		return nil, errors.New("missing named port `metrics` for the service associated with the target inferencepool")
	}

	epp := EndpointPicker{
		Namespace:         inferencePool.Namespace,
		ServiceName:       serviceName,
		MetricsPortNumber: portNumber,
	}

	selector := make(map[string]string, len(inferencePool.Spec.Selector.MatchLabels))
	for k, v := range inferencePool.Spec.Selector.MatchLabels {
		selector[string(k)] = string(v)
	}
	endpointPool := &EndpointPool{
		Selector:       selector,
		Namespace:      inferencePool.Namespace,
		Name:           inferencePool.Name,
		EndpointPicker: epp,
	}
	return endpointPool, nil
}

// GetLabelValueHash takes a list of labels and extract the values, sorts them, concatenates them,
// and returns the SHA256 hash of the resulting string. This function will help us to find the target
// InferencePool given the labels for a pod attached to the inferencePool
func GetLabelValueHash(labels map[string]string) string {
	valueList := []string{}
	for _, value := range labels {
		valueList = append(valueList, value)
	}
	// Sort the strings alphabetically
	sort.Strings(valueList)
	concatenatedString := strings.Join(valueList, "")

	// Calculate the SHA256 hash of the concatenated string
	hasher := sha256.New()
	hasher.Write([]byte(concatenatedString))
	hashBytes := hasher.Sum(nil)

	// Return the hash as a hexadecimal string
	return hex.EncodeToString(hashBytes)
}

func GKNN() common.GKNN {
	var (
		DefaultPoolGroup     = "inference.networking.k8s.io"
		DefaultPoolName      = "defaultPool"
		DefaultPoolNamespace = "default"
	)

	gknn := common.GKNN{
		NamespacedName: types.NamespacedName{Name: DefaultPoolName, Namespace: DefaultPoolNamespace},
		GroupKind: schema.GroupKind{
			Group: DefaultPoolGroup,
			Kind:  "InferencePool",
		},
	}
	return gknn
}
