package worker

import (
	"context"
	"fmt"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer
	enableWorkerStateTracking dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func newHandler(dc *dynamicconfig.Collection) *handler {
	return &handler{
		enableWorkerStateTracking: dynamicconfig.EnableWorkerStateTracking.Get(dc),
	}
}

func (h *handler) RecordHeartbeat(ctx context.Context, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
	// Check if worker state tracking is enabled for this namespace
	if !h.enableWorkerStateTracking(req.NamespaceId) {
		// Worker state tracking is disabled, return error
		return nil, fmt.Errorf("worker state tracking is disabled for namespace %s", req.NamespaceId)
	}

	// Validate that exactly one worker heartbeat is present
	frontendReq := req.GetFrontendRequest()
	if frontendReq == nil || len(frontendReq.GetWorkerHeartbeat()) != 1 {
		return nil, fmt.Errorf("exactly one worker heartbeat must be present in the request")
	}

	workerHeartbeat := frontendReq.GetWorkerHeartbeat()[0]

	// Try to update existing worker, or create new one if it doesn't exist
	_, _, _, _, err := chasm.UpdateWithNewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  workerHeartbeat.WorkerInstanceKey,
		},
		func(ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*Worker, *workerstatepb.RecordHeartbeatResponse, error) {
			// Create new worker and record heartbeat
			w := NewWorker()
			err := w.recordHeartbeat(ctx, req)
			if err != nil {
				return nil, nil, err
			}
			return w, &workerstatepb.RecordHeartbeatResponse{}, nil
		},
		func(w *Worker, ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
			// Update existing worker with new heartbeat
			err := w.recordHeartbeat(ctx, req)
			if err != nil {
				return nil, err
			}
			return &workerstatepb.RecordHeartbeatResponse{}, nil
		},
		req,
	)

	if err != nil {
		return nil, err
	}

	return &workerstatepb.RecordHeartbeatResponse{}, nil
}
