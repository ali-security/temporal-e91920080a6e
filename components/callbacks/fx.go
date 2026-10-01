package callbacks

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.callbacks",
	fx.Provide(ConfigProvider),
	fx.Provide(HTTPCallerProviderProviderWithConfig),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)

// HTTPCallerProviderProvider provides an HTTPCallerProvider that inspects the legacy "source"
// header according to the default value of the callback.inspectSourceHeader setting.
// Prefer HTTPCallerProviderProviderWithConfig, which honors the dynamically configured value.
func HTTPCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
) (HTTPCallerProvider, error) {
	return newHTTPCallerProvider(
		clusterMetadata,
		namespaceRegistry,
		rpcFactory,
		httpClientCache,
		logger,
		defaultInspectSourceHeader(),
	)
}

// HTTPCallerProviderProviderWithConfig provides an HTTPCallerProvider that honors the
// callback.inspectSourceHeader dynamic config setting.
func HTTPCallerProviderProviderWithConfig(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
	config *Config,
) (HTTPCallerProvider, error) {
	return newHTTPCallerProvider(
		clusterMetadata,
		namespaceRegistry,
		rpcFactory,
		httpClientCache,
		logger,
		config.InspectSourceHeader(),
	)
}

func newHTTPCallerProvider(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
	inspectSourceHeader bool,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	defaultClient := &http.Client{}
	callbackTokenGenerator := commonnexus.NewCallbackTokenGenerator()

	m := collection.NewOnceMap(func(queuescommon.NamespaceIDAndDestination) HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return routeRequest(r,
				clusterMetadata,
				namespaceRegistry,
				httpClientCache,
				callbackTokenGenerator,
				defaultClient,
				localClient,
				logger,
				inspectSourceHeader,
			)
		}
	})
	return m.Get, nil
}
