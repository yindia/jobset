/*
Copyright The Kubernetes Authors.
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

package controllers

import (
	"context"
	"fmt"
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	"sigs.k8s.io/jobset/pkg/constants"
	"sigs.k8s.io/jobset/pkg/features"
	"sigs.k8s.io/jobset/pkg/metrics"
)

// syncActiveDeadlineStartTime maintains .status.startTime, the JobSet's general
// active-start timestamp (also the anchor for spec.activeDeadlineSeconds).
//
// It matches batch/v1.Job semantics and is maintained regardless of whether
// activeDeadlineSeconds is set or the JobSetActiveDeadlineSeconds gate is on:
//   - set to now when the JobSet is active (unsuspended) and startTime is unset,
//   - cleared when the JobSet is suspended.
//
// Because suspend clears startTime, resume naturally re-sets it on the next
// active reconcile. Global restarts reset it explicitly in the reconcile loop via
// resetStartTimeOnGlobalRestart, which fires when Status.Restarts increases.
func (r *JobSetReconciler) syncActiveDeadlineStartTime(js *jobset.JobSet, updateStatusOpts *statusUpdateOpts) {
	if jobSetSuspended(js) {
		if js.Status.StartTime != nil {
			js.Status.StartTime = nil
			updateStatusOpts.shouldUpdate = true
		}
		return
	}
	if js.Status.StartTime == nil {
		now := metav1.NewTime(r.clock.Now())
		js.Status.StartTime = &now
		updateStatusOpts.shouldUpdate = true
	}
}

// resetStartTimeOnGlobalRestart resets .status.startTime to now when the failure
// policy performed a global restart in this reconcile (detected by
// Status.Restarts increasing), since a global restart begins a fresh run. It is
// a no-op for single-Job restarts (which do not bump Status.Restarts) and
// terminal failures. Like the rest of startTime maintenance it is ungated and
// uses the injectable clock.
func (r *JobSetReconciler) resetStartTimeOnGlobalRestart(js *jobset.JobSet, restartsBefore int32, updateStatusOpts *statusUpdateOpts) {
	if js.Status.Restarts > restartsBefore && js.Status.StartTime != nil {
		now := metav1.NewTime(r.clock.Now())
		js.Status.StartTime = &now
		updateStatusOpts.shouldUpdate = true
	}
}

// executeActiveDeadlinePolicy enforces spec.activeDeadlineSeconds against
// .status.startTime. It returns whether the deadline has expired, and if not,
// the duration after which the JobSet should be requeued so the controller wakes
// exactly at expiry (a wedged JobSet produces no child events on its own).
//
// On expiry the JobSet is marked Failed with reason DeadlineExceeded and its
// active child Jobs are deleted in the same reconcile, freeing resources promptly.
func (r *JobSetReconciler) executeActiveDeadlinePolicy(ctx context.Context, js *jobset.JobSet, ownedJobs *childJobs, updateStatusOpts *statusUpdateOpts) (bool, time.Duration, error) {
	log := ctrl.LoggerFrom(ctx)

	if !features.Enabled(features.JobSetActiveDeadlineSeconds) {
		return false, 0, nil
	}
	if js.Spec.ActiveDeadlineSeconds == nil {
		return false, 0, nil
	}
	// The deadline measures continuous active time; it does not accrue while suspended.
	if jobSetSuspended(js) {
		return false, 0, nil
	}
	if js.Status.StartTime == nil {
		return false, 0, nil
	}

	adls := *js.Spec.ActiveDeadlineSeconds
	// Guard against int64 nanosecond overflow for very large deadlines. Such a
	// JobSet is effectively unbounded, so treat it as never expiring rather than
	// wrapping to a negative duration (which would fail it immediately).
	if adls > math.MaxInt64/int64(time.Second) {
		return false, 0, nil
	}

	deadline := js.Status.StartTime.Add(time.Duration(adls) * time.Second)
	remaining := deadline.Sub(r.clock.Now())
	if remaining > 0 {
		// Not expired yet (also covers clock skew where startTime is in the future).
		return false, remaining, nil
	}

	// Deadline exceeded. Delete the active child Jobs first, then record the
	// failure, so the Failed condition and metric are only emitted once the
	// resources are actually freed. If deletion fails we return the error without
	// touching the condition or metric; the next reconcile retries from a
	// not-yet-failed state, avoiding metric inflation.
	if err := r.deleteJobs(ctx, ownedJobs.active); err != nil {
		return true, 0, err
	}
	msg := fmt.Sprintf("JobSet was active for longer than the specified deadline of %d seconds", adls)
	log.V(2).Info("JobSet active deadline exceeded, failing JobSet", "activeDeadlineSeconds", adls)
	setJobSetFailedCondition(js, constants.DeadlineExceededReason, msg, updateStatusOpts)
	metrics.JobSetActiveDeadlineExceeded(js.Name, js.Namespace)
	return true, 0, nil
}
