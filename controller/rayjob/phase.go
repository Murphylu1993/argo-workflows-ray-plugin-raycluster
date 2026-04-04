package rayjob

import (
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/klog/v2"
)

// GetPhase converts a RayJob JobStatus to an Argo Workflow NodePhase.
func GetPhase(jobStatus rayv1.JobStatus) wfv1.NodePhase {
	switch jobStatus {
	case rayv1.JobStatusSucceeded:
		return wfv1.NodeSucceeded
	case rayv1.JobStatusFailed:
		return wfv1.NodeFailed
	case rayv1.JobStatusStopped:
		return wfv1.NodeFailed
	case rayv1.JobStatusRunning:
		return wfv1.NodeRunning
	case rayv1.JobStatusPending:
		return wfv1.NodePending
	default:
		klog.Infof("rayjob: unknown JobStatus %q, treating as pending", jobStatus)
		return wfv1.NodePending
	}
}
