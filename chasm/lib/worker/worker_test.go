package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tasktoken"
	"go.uber.org/mock/gomock"
)

// createValidTaskToken creates a valid serialized task token for testing.
func createValidTaskToken(t *testing.T, namespaceID, workflowID, runID, activityID string) []byte {
	token := &tokenspb.Task{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivityId:  activityID,
	}
	serializer := tasktoken.NewSerializer()
	taskToken, err := serializer.Serialize(token)
	require.NoError(t, err)
	return taskToken
}

func TestNewWorker(t *testing.T) {
	worker := NewWorker()

	// Verify basic initialization
	require.NotNil(t, worker)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Nil(t, worker.LeaseExpirationTime)
	require.Nil(t, worker.WorkerHeartbeat) // No heartbeat data initially
	require.Empty(t, worker.workerID())    // Empty until first heartbeat
}

func TestWorkerLifecycleState(t *testing.T) {
	worker := NewWorker()
	var ctx chasm.Context

	// Test ACTIVE -> Running
	state := worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test INACTIVE -> Running (still running until cleanup)
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test CLEANED_UP -> Completed
	worker.Status = workerstatepb.WORKER_STATUS_CLEANED_UP
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, state)
}

func TestWorkerRecordHeartbeat(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: "test-namespace-id",
		FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
				{
					WorkerInstanceKey: "test-worker-3",
				},
			},
			// LeaseDuration: durationpb.New(30 * time.Second), // Will be available after proto regeneration
		},
	}

	// Test recording heartbeat on new worker
	resp, err := worker.recordHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify heartbeat data was set
	require.Equal(t, "test-worker-3", worker.workerID())

	// Verify lease expiration time was set
	require.NotNil(t, worker.LeaseExpirationTime)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled (lease expiry)
	require.Len(t, ctx.Tasks, 1)
}

func TestRescheduleActivities(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	logger := log.NewNoopLogger()
	ctx := context.Background()

	t.Run("NoActivities", func(t *testing.T) {
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}

		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.NoError(t, err)
	})

	t.Run("EmptyActivityList", func(t *testing.T) {
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
			ActivityInfo: &workerpb.ActivityInfo{
				RunningActivityIds: [][]byte{},
			},
		}

		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.NoError(t, err)
	})

	t.Run("SuccessfulReschedule", func(t *testing.T) {
		worker := NewWorker()
		// Create a valid serialized task token
		taskToken := createValidTaskToken(t, "namespace-id", "workflow-id", "run-id", "activity-id")
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
			ActivityInfo: &workerpb.ActivityInfo{
				RunningActivityIds: [][]byte{taskToken},
			},
		}

		// Mock the history client to accept the timeout request
		mockHistoryClient.EXPECT().
			RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *historyservice.RespondActivityTaskFailedRequest, _ ...interface{}) (*historyservice.RespondActivityTaskFailedResponse, error) {
				require.Equal(t, taskToken, req.FailedRequest.TaskToken)
				require.NotNil(t, req.FailedRequest.Failure)
				require.Equal(t, "worker-liveness-monitor", req.FailedRequest.Identity)
				return &historyservice.RespondActivityTaskFailedResponse{}, nil
			}).
			Times(1)

		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.NoError(t, err)
	})

	t.Run("MultipleActivities", func(t *testing.T) {
		worker := NewWorker()
		taskToken1 := createValidTaskToken(t, "namespace-id", "workflow-id-1", "run-id-1", "activity-id-1")
		taskToken2 := createValidTaskToken(t, "namespace-id", "workflow-id-2", "run-id-2", "activity-id-2")
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
			ActivityInfo: &workerpb.ActivityInfo{
				RunningActivityIds: [][]byte{taskToken1, taskToken2},
			},
		}

		// Mock the history client to accept both timeout requests
		mockHistoryClient.EXPECT().
			RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
			Times(2).
			Return(&historyservice.RespondActivityTaskFailedResponse{}, nil)

		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.NoError(t, err)
	})

	t.Run("PartialFailure", func(t *testing.T) {
		worker := NewWorker()
		taskToken1 := createValidTaskToken(t, "namespace-id", "workflow-id-1", "run-id-1", "activity-id-1")
		taskToken2 := createValidTaskToken(t, "namespace-id", "workflow-id-2", "run-id-2", "activity-id-2")
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
			ActivityInfo: &workerpb.ActivityInfo{
				RunningActivityIds: [][]byte{taskToken1, taskToken2},
			},
		}

		// First call succeeds, second fails
		gomock.InOrder(
			mockHistoryClient.EXPECT().
				RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&historyservice.RespondActivityTaskFailedResponse{}, nil),
			mockHistoryClient.EXPECT().
				RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, context.DeadlineExceeded),
		)

		// Should not return error if at least one succeeds
		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.NoError(t, err)
	})

	t.Run("AllFailures", func(t *testing.T) {
		worker := NewWorker()
		taskToken1 := createValidTaskToken(t, "namespace-id", "workflow-id-1", "run-id-1", "activity-id-1")
		taskToken2 := createValidTaskToken(t, "namespace-id", "workflow-id-2", "run-id-2", "activity-id-2")
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
			ActivityInfo: &workerpb.ActivityInfo{
				RunningActivityIds: [][]byte{taskToken1, taskToken2},
			},
		}

		// Both calls fail
		mockHistoryClient.EXPECT().
			RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
			Times(2).
			Return(nil, context.DeadlineExceeded)

		// Should return error if all fail
		err := worker.rescheduleActivities(ctx, logger, mockHistoryClient)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to timeout all")
	})
}
