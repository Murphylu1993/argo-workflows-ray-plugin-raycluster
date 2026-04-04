package raycluster

import (
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// GetPhase derives the Argo NodePhase from a RayCluster's status.
// For long-running Ray clusters, a healthy/ready cluster is represented as NodeRunning.
func GetPhase(cluster *rayv1.RayCluster) wfv1.NodePhase {
	switch cluster.Status.State {
	case rayv1.Ready:
		return wfv1.NodeRunning
	case rayv1.Failed:
		return wfv1.NodeFailed
	case rayv1.Suspended:
		return wfv1.NodeSucceeded
	default:
		if isConditionTrue(cluster, rayv1.HeadPodReady) || isConditionTrue(cluster, rayv1.RayClusterProvisioned) {
			return wfv1.NodeRunning
		}
		return wfv1.NodePending
	}
}

// isConditionTrue returns true if the given condition type is present and True on the cluster.
func isConditionTrue(cluster *rayv1.RayCluster, condType rayv1.RayClusterConditionType) bool {
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == string(condType) && condition.Status == metav1.ConditionTrue {
			klog.V(4).Infof("RayCluster %s condition %s is True", cluster.Name, condType)
			return true
		}
	}
	return false
}
