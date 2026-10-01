package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runSentinelHandlerTestCase asserts that the given operation returns
// NotFound when invoked on a sentinel scheduler.
func runSentinelHandlerTestCase(
	t *testing.T,
	callFn func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error,
) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	specBuilder := legacyscheduler.NewSpecBuilder()

	err := callFn(sentinel, ctx, specBuilder)

	require.Error(t, err)
	var notFoundErr *serviceerror.NotFound
	require.ErrorAs(t, err, &notFoundErr, "expected NotFound error for sentinel")
}

func TestSentinelHandler_DescribeSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Describe(ctx, &schedulerpb.DescribeScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		}, specBuilder)
		return err
	})
}

func TestSentinelHandler_ListScheduleMatchingTimes(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.ListMatchingTimes(ctx, &schedulerpb.ListScheduleMatchingTimesRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				StartTime:  timestamppb.Now(),
				EndTime:    timestamppb.Now(),
			},
		}, specBuilder)
		return err
	})
}

// TestScheduler_ListMatchingTimes_ComputeLimitExceeded feeds the CHASM scheduler a mirrored
// include/exclude spec: the calendar matches every second and the exclude calendar blocks every
// second, so no candidate time is ever accepted. Before the compute bound existed, the
// ListScheduleMatchingTimes exclusion loop kept scanning one second at a time until
// maxCalendarYear (billions of iterations), pinning a CPU for the lifetime of the request. With
// the bound in place the request fails fast and non-retryably.
func TestScheduler_ListMatchingTimes_ComputeLimitExceeded(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	sched.Schedule.Spec = &schedulepb.ScheduleSpec{
		Calendar:        []*schedulepb.CalendarSpec{everySecond},
		ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
	}
	// Bust the spec compiled from the default (interval) schedule, which is cached on the
	// component and keyed off the conflict token.
	sched.ConflictToken++

	specBuilder := newLegacySpecBuilder(0, 1000)
	start := time.Now().UTC()
	listReq := func() *schedulerpb.ListScheduleMatchingTimesRequest {
		return &schedulerpb.ListScheduleMatchingTimesRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				StartTime:  timestamppb.New(start),
				EndTime:    timestamppb.New(start.Add(time.Hour)),
			},
		}
	}

	_, err := sched.ListMatchingTimes(ctx, listReq(), specBuilder)
	require.Error(t, err, "an over-excluded spec must not spin forever")
	require.ErrorIs(t, err, legacyscheduler.ErrScheduleSpecLimitHit)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr, "the compute limit must be reported as non-retryable")

	// A well-formed spec is unaffected and still enumerates matching times.
	sched.Schedule.Spec = &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}},
	}
	sched.ConflictToken++

	resp, err := sched.ListMatchingTimes(ctx, listReq(), specBuilder)
	require.NoError(t, err)
	require.Len(t, resp.FrontendResponse.GetStartTime(), 60)
}

func TestSentinelHandler_UpdateSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Update(ctx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_PatchSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Patch(ctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_DeleteSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Delete(ctx, &schedulerpb.DeleteScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_MigrateToWorkflow(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: namespaceID,
			ScheduleId:  scheduleID,
		})
		return err
	})
}

func TestHandler_CreateFromMigrationState_Sentinel(t *testing.T) {
	env := newTestEnv(t, withMockEngine())
	sentinel, ctx, _ := setupSentinelForTest(t)

	h := scheduler.NewTestHandler(env.Logger)

	// StartExecution returns already-started because the sentinel occupies the key.
	env.MockEngine.EXPECT().StartExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr("already exists", "", ""))

	// ReadComponent invokes the read function with the sentinel.
	env.ExpectReadComponent(ctx, sentinel)

	engineCtx := env.EngineContext()
	_, err := h.TestCreateFromMigrationState(engineCtx, &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				ScheduleId: scheduleID,
			},
		},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, scheduler.ErrSentinelBlocked)
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
}

func TestHandler_MigrateToWorkflow_Sentinel(t *testing.T) {
	env := newTestEnv(t, withMockEngine())
	sentinel, ctx, _ := setupSentinelForTest(t)

	h := scheduler.NewTestHandler(env.Logger)

	// UpdateComponent invokes the update function with the sentinel.
	env.ExpectUpdateComponent(ctx, sentinel)

	engineCtx := env.EngineContext()
	_, err := h.TestMigrateToWorkflow(engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, scheduler.ErrSentinelBlocked)
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
}
