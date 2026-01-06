/*
Copyright 2025 The llm-d Authors

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

package scalefromzero

import (
	"context"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/scale"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wvav1alpha1 "github.com/llm-d-incubation/workload-variant-autoscaler/api/v1alpha1"
	"github.com/llm-d-incubation/workload-variant-autoscaler/internal/engines/executor"
	"github.com/llm-d-incubation/workload-variant-autoscaler/internal/logging"
	"github.com/llm-d-incubation/workload-variant-autoscaler/internal/utils"
	poolutils "github.com/llm-d-incubation/workload-variant-autoscaler/internal/utils/pool"
)

// NOTE: This is a placeholder for the scale-from-zero engine implementation.
// The actual logic for the scale-from-zero engine should be implemented here.

type Engine struct {
	client   client.Client
	executor executor.Executor
	// Add fields as necessary for the engine's state and configuration.
	Datastore     poolutils.Datastore
	DynamicClient *dynamic.DynamicClient
	ScaleClient   scale.ScalesGetter
	Mapper        meta.RESTMapper
}

// NewEngine creates a new instance of the scale-from-zero engine.
func NewEngine(client client.Client, config *rest.Config, datastore poolutils.Datastore) (*Engine, error) {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	scaleClient, mapper, err := poolutils.InitScaleClient(config)
	if err != nil {
		return nil, err
	}

	engine := Engine{
		client:        client,
		Datastore:     datastore,
		DynamicClient: dynamicClient,
		Mapper:        mapper,
		ScaleClient:   scaleClient,
	}

	// TODO: replace by an hybrid, polling and reactive executor when available
	engine.executor = executor.NewPollingExecutor(executor.PollingConfig{
		Config: executor.Config{
			OptimizeFunc: engine.optimize,
		},
		Interval:     100 * time.Millisecond, // frequent polling to quickly detect scale-from-zero opportunities
		RetryBackoff: 100 * time.Millisecond,
	})

	return &engine, nil
}

// StartOptimizeLoop starts the optimization loop for the scale-from-zero engine.
// It runs until the context is cancelled.
func (e *Engine) StartOptimizeLoop(ctx context.Context) {
	e.executor.Start(ctx)
}

// optimize performs the optimization logic.
func (e *Engine) optimize(ctx context.Context) error {
	// Get all inactive (replicas == 0) VAs
	inactiveVAs, err := utils.InactiveVariantAutoscaling(ctx, e.client)
	if err != nil {
		return err
	}

	ctrl.Log.V(logging.DEBUG).Info("Found inactive VariantAutoscaling resources", "count", len(inactiveVAs))

	var wg sync.WaitGroup
	const maxConcurrency = 20
	sem := make(chan struct{}, maxConcurrency)

	for _, va := range inactiveVAs {
		ctrl.Log.V(logging.DEBUG).Info("Processing variant", "name", va.Name)
		wg.Add(1)

		// This call blocks if the channel is full (concurrency limit reached)
		sem <- struct{}{}
		// go e.processVA(ctx, va, &wg, &sem)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			err := e.processVA(ctx, va)
			if err != nil {
				ctrl.Log.V(logging.DEBUG).Error(err, "Processing variant", "name", va.Name)
			}
		}()
	}

	wg.Wait()
	return nil
}

// optimize performs the optimization logic.
func (e *Engine) processVA(ctx context.Context, va wvav1alpha1.VariantAutoscaling) error {
	objAPI := va.GetScaleTargetAPI()
	objKind := va.GetScaleTargetKind()
	objName := va.GetScaleTargetName()

	// Parse Group, Version, Kind, Resource
	gvr, err := GetResourceForKind(e.Mapper, objAPI, objKind)
	if err != nil {
		//ctrl.Log.V(logging.DEBUG).Error(err, "Failed to parse Group, Version, Kind, Resource", "apiVersion", objAPI, "kind", objKind)
		return err
	}

	unstructuredObj, err := e.DynamicClient.Resource(gvr).Namespace(va.Namespace).Get(ctx, objName, metav1.GetOptions{})
	if err != nil {
		//ctrl.Log.V(logging.DEBUG).Error(err, "Error getting unstructured object")
		return err
	}

	//Extract Labels for the pods created by the ScaleTarget object
	result := unstructuredObj.Object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["labels"]

	var labels map[string]string
	err = mapstructure.Decode(result, &labels)

	if err != nil {
		//ctrl.Log.V(logging.DEBUG).Error(err, "Error converting labels interface to a map[string]string")
		return err
	}

	//Find inferencePool associated with pods created by the ScaleTarget object
	key := poolutils.GetLabelValueHash(labels)

	//Find target EPP for metrics collection
	pool, err := e.Datastore.PoolGetFromHashKey(key)
	if err != nil {
		//ctrl.Log.V(logging.DEBUG).Error(err, "Target inferencePool not found in the datastore")
		return err
	}

	epp := pool.EndpointPicker
	ctrl.Log.V(logging.DEBUG).Info("Target EPP service found", "name", epp.ServiceName)

	// TODO: Create EPP source and query metrics port
	return nil
}
