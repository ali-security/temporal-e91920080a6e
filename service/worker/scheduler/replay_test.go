package scheduler_test

import (
	"compress/gzip"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/scheduler"
)

// TestReplays tests workflow logic backwards compatibility from previous versions.
// Whenever there's a change in logic, consider capturing a new history with the
// testdata/generate_history.sh script and checking it in.
func TestReplays(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})

	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	logger := log.NewSdkLogger(log.NewTestLogger())

	for _, filename := range files {
		logger.Info("Replaying", "file", filename)
		f, err := os.Open(filename)
		require.NoError(t, err)
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
		require.NoError(t, err)
		err = replayer.ReplayWorkflowHistory(logger, history)
		require.NoError(t, err)
		_ = r.Close()
		_ = f.Close()
	}
}

// TestReplaysWithDynamicConfigChange proves that changing the compute-limit dynamic config
// (SchedulerSpecWarnIterations / SchedulerSpecMaxIterations) does not break replay of an existing
// schedule history. The scheduling decisions (next times, cache-completed) live in SideEffect /
// MutableSideEffect markers, so replay returns the recorded values rather than recomputing them
// against the new limits. Each checked-in history is replayed twice: once with the "original"
// (effectively disabled) bound, then again with a much lower bound simulating an operator
// lowering SchedulerSpecMaxIterations.
//
// Note: upstream also ships a testdata/replay_compute_limit_exceeded.json.gz fixture captured
// from a V1 workflow whose spec actually tripped the hard limit. That fixture is a binary blob
// that is not carried in the patch we backport from (the diff records it as "Binary files
// differ" with no payload) and it cannot be regenerated here without a running server, so this
// test runs over the existing replay_*.json.gz corpus instead. The determinism property being
// asserted — a config change must not alter replay — is the same for every history in it.
func TestReplaysWithDynamicConfigChange(t *testing.T) {
	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)
	require.NotEmpty(t, files, "expected checked-in replay histories to exercise")

	logger := log.NewSdkLogger(log.NewTestLogger())

	// loadHistory reads a fresh copy of the history. A fresh copy is needed per replay because
	// the replayer consumes/mutates the passed History.
	loadHistory := func(filename string) *historypb.History {
		f, err := os.Open(filename)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()
		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
		require.NoError(t, err)
		return history
	}

	// replayWith registers a scheduler workflow whose SpecBuilder reports the given hard bound,
	// then replays a fresh copy of the history against it.
	replayWith := func(maxIterations int, filename string) error {
		b := scheduler.NewSpecBuilderWithLimits(
			func() int { return scheduler.DefaultWarnIterations },
			func() int { return maxIterations },
		)
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(
			scheduler.SchedulerWorkflowWithSpecBuilder(b),
			workflow.RegisterOptions{Name: scheduler.WorkflowType},
		)
		return replayer.ReplayWorkflowHistory(logger, loadHistory(filename))
	}

	for _, filename := range files {
		logger.Info("Replaying", "file", filename)

		// Original config: hard limit effectively disabled.
		require.NoError(t, replayWith(math.MaxInt, filename),
			"replay with original config should succeed: %s", filename)

		// Simulated dynamic-config change: a much lower hard limit. Replay must still be
		// deterministic because the recorded next times come from SideEffect markers.
		require.NoError(t, replayWith(1000, filename),
			"replay after a dynamic-config change should still be deterministic: %s", filename)
	}
}
