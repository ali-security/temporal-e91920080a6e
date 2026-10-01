package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching/workers"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nexusMetricsCaptureEngine is a stub Engine for Nexus handler tests. It embeds
// the Engine interface so only the methods under test need to be implemented.
type nexusMetricsCaptureEngine struct {
	Engine // embedded to satisfy all other interface methods (will panic if called)
}

func (e *nexusMetricsCaptureEngine) Start() {}
func (e *nexusMetricsCaptureEngine) Stop()  {}

func (e *nexusMetricsCaptureEngine) PollNexusTaskQueue(
	_ context.Context,
	_ *matchingservice.PollNexusTaskQueueRequest,
	_ metrics.Handler,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	return &matchingservice.PollNexusTaskQueueResponse{}, nil
}

func (e *nexusMetricsCaptureEngine) RespondNexusTaskCompleted(
	_ context.Context,
	_ *matchingservice.RespondNexusTaskCompletedRequest,
	_ metrics.Handler,
) (*matchingservice.RespondNexusTaskCompletedResponse, error) {
	return &matchingservice.RespondNexusTaskCompletedResponse{}, nil
}

// ctxWithClientName creates a context with the given client-name set in incoming
// gRPC metadata and a deadline (required by PollNexusTaskQueue).
func ctxWithClientName(t *testing.T, clientName string) context.Context {
	t.Helper()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)
	md := metadata.New(map[string]string{
		headers.ClientNameHeaderName: clientName,
	})
	return metadata.NewIncomingContext(ctx, md)
}

// newTestHandler creates a Handler wired with the given capture handler, a stub
// engine, and a mock namespace registry that returns a fixed name.
func newTestHandler(t *testing.T, captureHandler *metricstest.CaptureHandler) (*Handler, *nexusMetricsCaptureEngine) {
	t.Helper()
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, assert.AnError).AnyTimes()

	stubEngine := &nexusMetricsCaptureEngine{}

	h := &Handler{
		engine:            stubEngine,
		config:            NewConfig(dynamicconfig.NewNoopCollection()),
		metricsHandler:    captureHandler,
		logger:            log.NewNoopLogger(),
		throttledLogger:   log.NewNoopLogger(),
		namespaceRegistry: nsRegistry,
	}
	// Mark handler as started so requests are served.
	h.startWG.Add(1)
	h.startWG.Done()

	return h, stubEngine
}

// findMetricWithTag searches captured metrics for a recording of the named metric
// that contains the specified tag key/value pair.
func findMetricWithTag(snap metricstest.CaptureSnapshot, metricName, tagKey, tagValue string) bool {
	for _, rec := range snap[metricName] {
		if v, ok := rec.Tags[tagKey]; ok && v == tagValue {
			return true
		}
	}
	return false
}

const nexusTaskRequestsMetric = "nexus_task_requests"

func TestNexusHandlersEmitClientNameMetric(t *testing.T) {
	const expectedClientName = "temporal-go"
	taskQueue := &taskqueuepb.TaskQueue{Name: "test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	t.Run("PollNexusTaskQueue", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, _ := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(t, expectedClientName)

		_, err := h.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: "test-ns-id",
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: taskQueue,
			},
		})
		require.NoError(t, err)

		snap := capture.Snapshot()
		require.NotEmpty(t, snap[nexusTaskRequestsMetric])
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "client_name", expectedClientName),
			"should have client_name tag, got: %v", snap)
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "operation", "PollNexusTaskQueue"),
			"should have operation tag, got: %v", snap)
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "is_internal", "false"),
			"should have is_internal=false, got: %v", snap)
	})

	t.Run("RespondNexusTaskCompleted", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, _ := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(t, expectedClientName)

		_, err := h.RespondNexusTaskCompleted(ctx, &matchingservice.RespondNexusTaskCompletedRequest{
			NamespaceId: "test-ns-id",
			TaskQueue:   taskQueue,
		})
		require.NoError(t, err)

		snap := capture.Snapshot()
		require.NotEmpty(t, snap[nexusTaskRequestsMetric])
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "client_name", expectedClientName),
			"should have client_name tag, got: %v", snap)
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "operation", "RespondNexusTaskCompleted"),
			"should have operation tag, got: %v", snap)
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "is_internal", "false"),
			"should have is_internal=false, got: %v", snap)
	})

	t.Run("no client_name when header absent", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, _ := newTestHandler(t, captureHandler)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := h.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: "test-ns-id",
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: taskQueue,
			},
		})
		require.NoError(t, err)

		snap := capture.Snapshot()
		require.NotEmpty(t, snap[nexusTaskRequestsMetric],
			"nexus_task_requests should still be emitted without client_name header")
		assert.False(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "client_name", expectedClientName),
			"should not have client_name tag when header is absent, got: %v", snap)
	})

	t.Run("is_internal for /temporal-sys/ task queue", func(t *testing.T) {
		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		h, _ := newTestHandler(t, captureHandler)
		ctx := ctxWithClientName(t, expectedClientName)
		internalTQ := &taskqueuepb.TaskQueue{
			Name: "/temporal-sys/worker-commands/ns/grouping-key",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		_, err := h.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
			NamespaceId: "test-ns-id",
			Request: &workflowservice.PollNexusTaskQueueRequest{
				TaskQueue: internalTQ,
			},
		})
		require.NoError(t, err)

		snap := capture.Snapshot()
		require.NotEmpty(t, snap[nexusTaskRequestsMetric])
		require.True(t, findMetricWithTag(snap, nexusTaskRequestsMetric, "is_internal", "true"),
			"should have is_internal=true for /temporal-sys/ task queue, got: %v", snap)
	})
}

func TestWorkerHeartbeatToListInfo_AllFieldsSet(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "instance-key-1",
		WorkerIdentity:    "worker-identity-1",
		HostInfo: &workerpb.WorkerHostInfo{
			HostName:          "host-1",
			WorkerGroupingKey: "grouping-key-1",
			ProcessId:         "pid-123",
		},
		TaskQueue:         "my-task-queue",
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "dep-1", BuildId: "build-1"},
		SdkName:           "temporal-go",
		SdkVersion:        "1.0.0",
		Status:            enumspb.WORKER_STATUS_RUNNING,
		StartTime:         timestamppb.Now(),
		Plugins:           []*workerpb.PluginInfo{{Name: "plugin-1"}},
		Drivers:           []*workerpb.StorageDriverInfo{{Type: "driver-1"}},
	}

	result := workerHeartbeatToListInfo(hb)
	require.NotNil(t, result)

	resultReflect := result.ProtoReflect()
	fields := resultReflect.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		val := resultReflect.Get(fd)
		if fd.IsList() {
			assert.NotEqualf(t, 0, val.List().Len(),
				"WorkerListInfo repeated field %q is empty — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		} else {
			assert.Truef(t, resultReflect.Has(fd),
				"WorkerListInfo field %q is not set by workerHeartbeatToListInfo — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		}
	}
}

// indexPageByCursor reproduces the class of bug the matching handler's panic
// handlers exist to contain: a cursor that arrives straight off the wire (a page
// token, a worker instance key, a namespace id, a task queue name) is trusted as
// a slice index without being bounds-checked. Any non-empty cursor indexes past
// the end of the page and raises a genuine Go runtime panic. Without a recover
// in the handler, that panic unwinds out of the gRPC handler goroutine and takes
// the whole matching service process down — a remote, unauthenticated crash.
func indexPageByCursor(cursor []byte) string {
	page := []string{"worker-0"}
	return page[len(cursor)]
}

// panickingWorkersRegistry is a workers.Registry whose methods route the
// caller-supplied cursor into indexPageByCursor, so the panic is reachable from
// the handler APIs that go through h.workersRegistry.
type panickingWorkersRegistry struct{}

func (r *panickingWorkersRegistry) RecordWorkerHeartbeats(
	nsID namespace.ID, _ namespace.Name, _ []*workerpb.WorkerHeartbeat,
) {
	_ = indexPageByCursor([]byte(nsID.String()))
}

func (r *panickingWorkersRegistry) ListWorkers(
	_ namespace.ID, params workers.ListWorkersParams,
) (workers.ListWorkersResponse, error) {
	_ = indexPageByCursor(params.NextPageToken)
	return workers.ListWorkersResponse{}, nil
}

func (r *panickingWorkersRegistry) DescribeWorker(
	_ namespace.ID, workerInstanceKey string,
) (*workerpb.WorkerHeartbeat, error) {
	_ = indexPageByCursor([]byte(workerInstanceKey))
	return &workerpb.WorkerHeartbeat{}, nil
}

// panickingEngine is a stub Engine for the handler APIs that forward to the
// engine. It embeds the Engine interface so only the methods under test need to
// be implemented.
type panickingEngine struct {
	Engine // embedded to satisfy all other interface methods (will panic if called)
}

func (e *panickingEngine) UpdateFairnessState(
	_ context.Context, request *matchingservice.UpdateFairnessStateRequest,
) (*matchingservice.UpdateFairnessStateResponse, error) {
	_ = indexPageByCursor([]byte(request.GetTaskQueue()))
	return &matchingservice.UpdateFairnessStateResponse{}, nil
}

func (e *panickingEngine) UpdateTaskQueueConfig(
	_ context.Context, request *matchingservice.UpdateTaskQueueConfigRequest,
) (*matchingservice.UpdateTaskQueueConfigResponse, error) {
	_ = indexPageByCursor([]byte(request.GetNamespaceId()))
	return &matchingservice.UpdateTaskQueueConfigResponse{}, nil
}

// newPanickingTestHandler builds a Handler whose engine and workers registry both
// panic on any non-empty cursor.
func newPanickingTestHandler(t *testing.T) *Handler {
	t.Helper()
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, assert.AnError).AnyTimes()

	h := &Handler{
		engine:            &panickingEngine{},
		config:            NewConfig(dynamicconfig.NewNoopCollection()),
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		throttledLogger:   log.NewNoopLogger(),
		namespaceRegistry: nsRegistry,
		workersRegistry:   &panickingWorkersRegistry{},
	}
	// Mark handler as started so requests are served.
	h.startWG.Add(1)
	h.startWG.Done()

	return h
}

// requireCapturedPanic asserts that the handler turned the downstream runtime
// panic into an Internal service error instead of letting it escape.
func requireCapturedPanic(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "the recovered panic must be reported back as an error")
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr,
		"captured panic must surface as *serviceerror.Internal, got %T: %v", err, err)
	require.Contains(t, internalErr.Error(), "index out of range",
		"error must carry the recovered runtime panic, got: %v", internalErr)
}

// TestWorkerHandlersCapturePanicFromDownstream is the regression test for the
// missing panic handlers on the matching worker APIs. Each subtest sends a
// request whose attacker-controlled cursor makes the layer below the handler
// panic; the handler must recover it and answer with an Internal error rather
// than crashing the matching service. Without the `defer log.CapturePanic`
// guards on these methods, every subtest panics out of the handler call.
func TestWorkerHandlersCapturePanicFromDownstream(t *testing.T) {
	// A forged, non-empty cursor is what pushes the index past the end of the page.
	const maliciousCursor = `{"LastWorkerInstanceKey":"worker-99999999"}`

	t.Run("ListWorkers", func(t *testing.T) {
		h := newPanickingTestHandler(t)

		var (
			resp *matchingservice.ListWorkersResponse
			err  error
		)
		require.NotPanics(t, func() {
			resp, err = h.ListWorkers(context.Background(), &matchingservice.ListWorkersRequest{
				NamespaceId: "test-ns-id",
				ListRequest: &workflowservice.ListWorkersRequest{
					NextPageToken: []byte(maliciousCursor),
				},
			})
		}, "a panic below ListWorkers must not escape the matching handler")
		require.Nil(t, resp)
		requireCapturedPanic(t, err)
	})

	t.Run("DescribeWorker", func(t *testing.T) {
		h := newPanickingTestHandler(t)

		var (
			resp *matchingservice.DescribeWorkerResponse
			err  error
		)
		require.NotPanics(t, func() {
			resp, err = h.DescribeWorker(context.Background(), &matchingservice.DescribeWorkerRequest{
				NamespaceId: "test-ns-id",
				Request: &workflowservice.DescribeWorkerRequest{
					WorkerInstanceKey: maliciousCursor,
				},
			})
		}, "a panic below DescribeWorker must not escape the matching handler")
		require.Nil(t, resp)
		requireCapturedPanic(t, err)
	})

	t.Run("RecordWorkerHeartbeat", func(t *testing.T) {
		h := newPanickingTestHandler(t)

		var (
			resp *matchingservice.RecordWorkerHeartbeatResponse
			err  error
		)
		require.NotPanics(t, func() {
			resp, err = h.RecordWorkerHeartbeat(context.Background(), &matchingservice.RecordWorkerHeartbeatRequest{
				NamespaceId: maliciousCursor,
			})
		}, "a panic below RecordWorkerHeartbeat must not escape the matching handler")
		require.Nil(t, resp)
		requireCapturedPanic(t, err)
	})

	t.Run("UpdateFairnessState", func(t *testing.T) {
		h := newPanickingTestHandler(t)

		var (
			resp *matchingservice.UpdateFairnessStateResponse
			err  error
		)
		require.NotPanics(t, func() {
			resp, err = h.UpdateFairnessState(context.Background(), &matchingservice.UpdateFairnessStateRequest{
				NamespaceId: "test-ns-id",
				TaskQueue:   maliciousCursor,
			})
		}, "a panic below UpdateFairnessState must not escape the matching handler")
		require.Nil(t, resp)
		requireCapturedPanic(t, err)
	})

	t.Run("UpdateTaskQueueConfig", func(t *testing.T) {
		h := newPanickingTestHandler(t)

		var (
			resp *matchingservice.UpdateTaskQueueConfigResponse
			err  error
		)
		require.NotPanics(t, func() {
			resp, err = h.UpdateTaskQueueConfig(context.Background(), &matchingservice.UpdateTaskQueueConfigRequest{
				NamespaceId: maliciousCursor,
			})
		}, "a panic below UpdateTaskQueueConfig must not escape the matching handler")
		require.Nil(t, resp)
		requireCapturedPanic(t, err)
	})
}
