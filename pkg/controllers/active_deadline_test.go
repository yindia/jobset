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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	clocktesting "k8s.io/utils/clock/testing"

	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	"sigs.k8s.io/jobset/pkg/constants"
	"sigs.k8s.io/jobset/pkg/features"
	testutils "sigs.k8s.io/jobset/pkg/util/testing"
)

func TestSyncActiveDeadlineStartTime(t *testing.T) {
	const (
		jobSetName = "test-jobset"
		ns         = "default"
	)
	now := metav1.Now()
	earlier := metav1.NewTime(now.Add(-time.Hour))

	tests := []struct {
		name           string
		jobset         *jobset.JobSet
		expectStartSet bool // startTime should be non-nil after sync
		expectCleared  bool // startTime was non-nil, expect nil after sync
		expectChanged  bool // shouldUpdate expected
		expectPreserve bool // startTime should keep its original value
	}{
		{
			name:           "active jobset with no startTime gets one",
			jobset:         testutils.MakeJobSet(jobSetName, ns).Obj(),
			expectStartSet: true,
			expectChanged:  true,
		},
		{
			name:           "active jobset with existing startTime is preserved",
			jobset:         testutils.MakeJobSet(jobSetName, ns).StartTime(earlier).Obj(),
			expectStartSet: true,
			expectPreserve: true,
		},
		{
			name:          "suspended jobset with startTime gets cleared",
			jobset:        testutils.MakeJobSet(jobSetName, ns).Suspend(true).StartTime(earlier).Obj(),
			expectCleared: true,
			expectChanged: true,
		},
		{
			name:   "suspended jobset with no startTime stays nil",
			jobset: testutils.MakeJobSet(jobSetName, ns).Suspend(true).Obj(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &JobSetReconciler{clock: clocktesting.NewFakeClock(now.Time)}
			opts := &statusUpdateOpts{}
			orig := tc.jobset.Status.StartTime.DeepCopy()

			r.syncActiveDeadlineStartTime(tc.jobset, opts)

			got := tc.jobset.Status.StartTime
			if tc.expectStartSet {
				require.NotNil(t, got, "expected startTime to be set")
			}
			if tc.expectCleared {
				require.Nil(t, got, "expected startTime to be cleared")
			}
			if !tc.expectStartSet && !tc.expectCleared {
				require.Nil(t, got, "expected startTime to stay nil")
			}
			if tc.expectPreserve {
				require.NotNil(t, got)
				require.True(t, got.Equal(orig), "expected startTime %v to be preserved, got %v", orig, got)
			}
			require.Equal(t, tc.expectChanged, opts.shouldUpdate)
		})
	}
}

func TestResetStartTimeOnGlobalRestart(t *testing.T) {
	const (
		jobSetName = "test-jobset"
		ns         = "default"
	)
	now := metav1.Now()
	old := metav1.NewTime(now.Add(-time.Hour))

	tests := []struct {
		name           string
		restartsBefore int32
		restartsAfter  int32
		startTime      *metav1.Time
		expectReset    bool
		expectChanged  bool
	}{
		{
			name:           "global restart bumps restarts: startTime reset to now",
			restartsBefore: 2,
			restartsAfter:  3,
			startTime:      &old,
			expectReset:    true,
			expectChanged:  true,
		},
		{
			name:           "single-Job restart (restarts unchanged): startTime untouched",
			restartsBefore: 2,
			restartsAfter:  2,
			startTime:      &old,
		},
		{
			name:           "restart bumped but startTime nil (suspended): stays nil",
			restartsBefore: 2,
			restartsAfter:  3,
			startTime:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &JobSetReconciler{clock: clocktesting.NewFakeClock(now.Time)}
			js := testutils.MakeJobSet(jobSetName, ns).Obj()
			js.Status.StartTime = tc.startTime
			js.Status.Restarts = tc.restartsAfter
			opts := &statusUpdateOpts{}

			r.resetStartTimeOnGlobalRestart(js, tc.restartsBefore, opts)

			if tc.expectReset {
				require.NotNil(t, js.Status.StartTime)
				require.True(t, js.Status.StartTime.Time.Equal(now.Time), "expected startTime reset to %v, got %v", now.Time, js.Status.StartTime)
			} else if tc.startTime == nil {
				require.Nil(t, js.Status.StartTime, "expected startTime to stay nil")
			} else {
				require.True(t, js.Status.StartTime.Equal(tc.startTime), "expected startTime unchanged %v, got %v", tc.startTime, js.Status.StartTime)
			}
			require.Equal(t, tc.expectChanged, opts.shouldUpdate)
		})
	}
}

func TestExecuteActiveDeadlinePolicy(t *testing.T) {
	const (
		jobSetName = "test-jobset"
		ns         = "default"
	)
	now := metav1.Now()

	tests := []struct {
		name                string
		gateEnabled         bool
		jobset              *jobset.JobSet
		expectExpired       bool
		expectRequeueApprox time.Duration // >0 means requeue expected roughly equal
		expectFailed        bool
	}{
		{
			name:        "gate disabled: no-op even when deadline set and elapsed",
			gateEnabled: false,
			jobset: testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(10).
				StartTime(metav1.NewTime(now.Add(-time.Hour))).Obj(),
		},
		{
			name:        "deadline unset: no-op",
			gateEnabled: true,
			jobset:      testutils.MakeJobSet(jobSetName, ns).StartTime(now).Obj(),
		},
		{
			name:        "suspended: no-op",
			gateEnabled: true,
			jobset: testutils.MakeJobSet(jobSetName, ns).Suspend(true).ActiveDeadlineSeconds(10).
				StartTime(metav1.NewTime(now.Add(-time.Hour))).Obj(),
		},
		{
			name:        "not started (nil startTime): no-op",
			gateEnabled: true,
			jobset:      testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(10).Obj(),
		},
		{
			name:        "remaining > 0: requeue, no failure",
			gateEnabled: true,
			jobset: testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(60).
				StartTime(metav1.NewTime(now.Add(-10 * time.Second))).Obj(),
			expectRequeueApprox: 50 * time.Second,
		},
		{
			name:        "deadline exceeded: fail",
			gateEnabled: true,
			jobset: testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(10).
				StartTime(metav1.NewTime(now.Add(-time.Hour))).Obj(),
			expectExpired: true,
			expectFailed:  true,
		},
		{
			name:        "future startTime (clock skew): treated as not expired",
			gateEnabled: true,
			jobset: testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(10).
				StartTime(metav1.NewTime(now.Add(time.Hour))).Obj(),
			expectRequeueApprox: time.Hour + 10*time.Second,
		},
		{
			name:        "overflow-large deadline: treated as never expiring, no failure",
			gateEnabled: true,
			jobset: testutils.MakeJobSet(jobSetName, ns).ActiveDeadlineSeconds(math.MaxInt64).
				StartTime(metav1.NewTime(now.Add(-time.Hour))).Obj(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			features.SetFeatureGateDuringTest(t, features.JobSetActiveDeadlineSeconds, tc.gateEnabled)

			r := &JobSetReconciler{clock: clocktesting.NewFakeClock(now.Time)}
			opts := &statusUpdateOpts{}
			// No active jobs, so deleteJobs is a no-op and needs no client.
			expired, requeueAfter, err := r.executeActiveDeadlinePolicy(ctx, tc.jobset, &childJobs{}, opts)
			require.NoError(t, err)
			require.Equal(t, tc.expectExpired, expired)
			if tc.expectRequeueApprox > 0 {
				// Allow small slack for test execution time.
				diff := requeueAfter - tc.expectRequeueApprox
				require.True(t, diff >= -time.Second && diff <= time.Second, "expected requeueAfter ~%v, got %v", tc.expectRequeueApprox, requeueAfter)
			} else if !tc.expectExpired {
				require.Zero(t, requeueAfter, "expected no requeue")
			}
			failed := apimeta.IsStatusConditionTrue(tc.jobset.Status.Conditions, string(jobset.JobSetFailed))
			require.Equal(t, tc.expectFailed, failed)
			if tc.expectFailed {
				cond := apimeta.FindStatusCondition(tc.jobset.Status.Conditions, string(jobset.JobSetFailed))
				require.NotNil(t, cond)
				require.Equal(t, constants.DeadlineExceededReason, cond.Reason)
			}
		})
	}
}
