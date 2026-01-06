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
	"errors"
	"sync"
)

var (
	errPoolNotSynced = errors.New("EndpointPool is not initialized in data store")
)

// The datastore is a local cache of relevant data for the given InferencePool (currently all pulled from k8s-api)
type Datastore interface {
	// InferencePool operations
	PoolSet(pool *EndpointPool)
	PoolGet(name string) (*EndpointPool, error)
	PoolList() []EndpointPool
	PoolGetFromHashKey(key string) (*EndpointPool, error)
	PoolDelete(poolName string)

	// Clears the store state, happens when the pool gets deleted.
	Clear()
}

func NewDatastore() Datastore {
	store := &datastore{
		pools:      &sync.Map{},
		labelCache: &sync.Map{},
	}
	return store
}

type datastore struct {
	pools      *sync.Map
	labelCache *sync.Map // stores mapping of pod labels --> inferencePool name

}

// Datastore operations
func (ds *datastore) PoolSet(pool *EndpointPool) {
	ds.pools.Store(pool.Name, pool)

	// store mapping of labels to inferencePools
	key := GetLabelValueHash(pool.Selector)
	ds.labelCache.Store(key, pool.Name)
}

func (ds *datastore) PoolGet(name string) (*EndpointPool, error) {

	pool, exist := ds.pools.Load(name)
	if !exist {
		return nil, errPoolNotSynced
	}

	epp := pool.(EndpointPool)
	return &epp, nil
}

func (ds *datastore) PoolGetFromHashKey(key string) (*EndpointPool, error) {
	poolName, exist := ds.labelCache.Load(key)
	if !exist {
		return nil, errPoolNotSynced
	}
	return ds.PoolGet(poolName.(string))
}

func (ds *datastore) PoolList() []EndpointPool {
	res := []EndpointPool{}
	ds.pools.Range(func(k, v any) bool {
		res = append(res, v.(EndpointPool))
		return true
	})

	return res
}

func (ds *datastore) PoolDelete(name string) {
	pool, err := ds.PoolGet(name)
	if err == nil {
		key := GetLabelValueHash(pool.Selector)
		ds.labelCache.Delete(key)
	}

	ds.pools.Delete(name)
}

func (ds *datastore) Clear() {
	ds.pools.Clear()
	ds.labelCache.Clear()
}
