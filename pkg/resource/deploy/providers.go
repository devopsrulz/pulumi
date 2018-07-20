// Copyright 2016-2018, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deploy

import (
	"fmt"

	"github.com/blang/semver"
	"github.com/pkg/errors"

	"github.com/pulumi/pulumi/pkg/resource"
	"github.com/pulumi/pulumi/pkg/resource/config"
	"github.com/pulumi/pulumi/pkg/resource/plugin"
	"github.com/pulumi/pulumi/pkg/tokens"
	"github.com/pulumi/pulumi/pkg/util/contract"
	"github.com/pulumi/pulumi/pkg/util/logging"
	"github.com/pulumi/pulumi/pkg/workspace"
)

var ErrMissingProvider = errors.New("provider not found")

// ProviderSource allows access to providers at runtime.
type ProviderSource interface {
	// GetProvider returns the provider plugin for the given URN.
	GetProvider(urn resource.URN) (plugin.Provider, error)
}

type providerLoadResponse struct {
	provider plugin.Provider
	failures []plugin.CheckFailure
	err error
}

type providerLoadRequest struct {
	urn        resource.URN
	properties resource.PropertyMap
	response   chan<- providerLoadResponse
}

type providerRecord struct {
	properties resource.PropertyMap
	provider   plugin.Provider
}

type providerLoader struct {
	host plugin.Host
	providers map[resource.URN]providerRecord // the map from plugin URN to plugin instance.
}

func (p *providerLoader) loadProvider(urn resource.URN,
	properties resource.PropertyMap) (plugin.Provider, []plugin.CheckFailure, error) {

	logging.V(7).Infof("loading provider %v", urn)

	// Extract the requested version from the properties if present.
	var failures []plugin.CheckFailure
	var version *semver.Version
	if versionProp, ok := properties["version"]; ok {
		if !versionProp.IsString() {
			failures = append(failures, plugin.CheckFailure{
				Property: "version",
				Reason: "'version' must be a string",
			})
		} else {
			sv, err := semver.ParseTolerant(versionProp.StringValue())
			if err != nil {
				failures = append(failures, plugin.CheckFailure{
					Property: "version",
					Reason: fmt.Sprintf("could not parse provider version: %v", err),
				})
			}
			version = &sv
		}
	}

	// Convert the property map to a provider config map, removing reserved properties.
	cfg := make(map[config.Key]string)
	for k, v := range properties {
		if k == "version" {
			continue
		}

		switch {
		case v.IsComputed():
			failures = append(failures, plugin.CheckFailure{
				Property: k,
				Reason: "provider properties must not be unknown",
			})
		case v.IsString():
			key := config.MustMakeKey(string(urn.Type().Name()), string(k))
			cfg[key] = v.StringValue()
		default:
			failures = append(failures, plugin.CheckFailure{
				Property: k,
				Reason: "provider property values must be strings",
			})
		}
	}

	// If there were any validation failures, return them now.
	if len(failures) != 0 {
		return nil, failures, nil
	}

	// Load the plugin.
	provider, err := p.host.Provider(tokens.Package(urn.Type().Name()), version)
	if err != nil {
		return nil, nil, err
	}

	// Attempt to configure the plugin. If configuration fails, discard the loaded plugin.
	if err = provider.Configure(cfg); err != nil {
		closeErr := p.host.CloseProvider(provider)
		if closeErr != nil {
			logging.Infof("Error closing provider; ignoring: %v", closeErr)
		}
		return nil, nil, err
	}

	logging.V(7).Infof("loaded provider %v", urn)

	// Return the loaded and configured plugin.
	return provider, nil, nil
}

func (p *providerLoader) serve(requests <-chan providerLoadRequest) {
	for req := range requests {
		record, ok := p.providers[req.urn]
		if req.properties == nil {
			if !ok {
				req.response <- providerLoadResponse{err: ErrMissingProvider}
			} else {
				req.response <- providerLoadResponse{provider: record.provider}
			}
		} else {
			contract.Assert(!ok)
			provider, failures, err := p.loadProvider(req.urn, req.properties)
			if len(failures) == 0 && err == nil {
				p.providers[req.urn] = providerRecord{
					properties: req.properties.Copy(),
					provider: provider,
				}
			}

			req.response <- providerLoadResponse{
				provider: provider,
				failures: failures,
				err: err,
			}
		}
	}
}

type metaProvider struct {
	loadRequests chan<- providerLoadRequest
}

func newMetaProvider(host plugin.Host) *metaProvider {
	loader := &providerLoader{
		host: host,
		providers: make(map[resource.URN]providerRecord),
	}
	loadRequests := make(chan providerLoadRequest)
	go loader.serve(loadRequests)

	return &metaProvider{loadRequests: loadRequests}
}

func (p *metaProvider) getProvider(urn resource.URN) (plugin.Provider, error) {
	logging.V(7).Infof("getting provider %v", urn)

	provider, _, err := p.loadProvider(urn, nil)
	return provider, err
}

func (p *metaProvider) loadProvider(urn resource.URN,
	properties resource.PropertyMap) (plugin.Provider, []plugin.CheckFailure, error) {

	resp := make(chan providerLoadResponse)
	defer close(resp)

	go func() {
		p.loadRequests <- providerLoadRequest{
			urn: urn,
			properties: properties,
			response: resp,
		}
	}()
	response := <-resp

	return response.provider, response.failures, response.err
}

func (p *metaProvider) Close() error {
	return nil
}

func (p *metaProvider) Pkg() tokens.Package {
	return "pulumi-providers"
}

func (p *metaProvider) Configure(props map[config.Key]string) error {
	contract.Fail()
	return errors.New("the metaProvider is not configurable")
}

func (p *metaProvider) Check(urn resource.URN, olds, news resource.PropertyMap,
	allowUnknowns bool) (resource.PropertyMap, []plugin.CheckFailure, error) {

	_, failures, err := p.loadProvider(urn, news)
	return news, failures, err
}

func (p *metaProvider) Diff(urn resource.URN, id resource.ID, olds, news resource.PropertyMap,
	allowUnknowns bool) (plugin.DiffResult, error) {
	// never require replacement
	return plugin.DiffResult{Changes: plugin.DiffUnknown}, nil
}

func (p *metaProvider) Create(urn resource.URN,
	news resource.PropertyMap) (resource.ID, resource.PropertyMap, resource.Status, error) {

	if _, err := p.getProvider(urn); err != nil {
		return "", nil, resource.StatusOK, err
	}
	return "0", resource.PropertyMap{}, resource.StatusOK, nil
}

func (p *metaProvider) Read(urn resource.URN, id resource.ID,
	props resource.PropertyMap) (resource.PropertyMap, error) {
	contract.Fail()
	return nil, errors.New("providers may not be read")
}

func (p *metaProvider) Update(urn resource.URN, id resource.ID, olds,
	news resource.PropertyMap) (resource.PropertyMap, resource.Status, error) {

	if _, err := p.getProvider(urn); err != nil {
		return nil, resource.StatusOK, err
	}
	return resource.PropertyMap{}, resource.StatusOK, nil
}

func (p *metaProvider) Delete(urn resource.URN, id resource.ID, props resource.PropertyMap) (resource.Status, error) {
	return resource.StatusOK, nil
}

func (p *metaProvider) Invoke(tok tokens.ModuleMember,
	args resource.PropertyMap) (resource.PropertyMap, []plugin.CheckFailure, error) {
	contract.Fail()
	return nil, nil, errors.New("the metaProvider is not invokeable")
}

func (p *metaProvider) GetPluginInfo() (workspace.PluginInfo, error) {
	// return an error: this should not be called for the metaProvider
	contract.Fail()
	return workspace.PluginInfo{}, errors.New("the metaProvider does not report plugin info")
}

func (p *metaProvider) SignalCancellation() error {
	// TODO: this should probably cancel any outstanding load requests and return
	return nil
}
