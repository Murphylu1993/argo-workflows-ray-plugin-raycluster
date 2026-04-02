package controller

import (
	"net/http"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	executorplugins "github.com/argoproj/argo-workflows/v3/pkg/plugins/executor"
	"github.com/gin-gonic/gin"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// RayJobPluginBody is the plugin body for RayJob templates.
type RayJobPluginBody struct {
	RayJob *rayv1.RayJob `json:"ray"`
}

// InjectRayJobWithWorkflowName injects the workflow name label into a RayJob.
func InjectRayJobWithWorkflowName(job *rayv1.RayJob, workflowName string) {
	if job == nil {
		return
	}
	if job.ObjectMeta.Labels != nil {
		job.ObjectMeta.Labels[LabelKeyWorkflow] = workflowName
	} else {
		job.ObjectMeta.Labels = map[string]string{
			LabelKeyWorkflow: workflowName,
		}
	}
	// Also inject labels into RayClusterSpec head if present
	if job.Spec.RayClusterSpec != nil {
		headLabels := job.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta.Labels
		if headLabels == nil {
			headLabels = map[string]string{}
		}
		headLabels[LabelKeyWorkflow] = workflowName
		job.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta.Labels = headLabels
	}
}

// ResponseJobCreated responds with Pending status after creating a new RayJob.
func (ct *RayClusterController) ResponseJobCreated(ctx *gin.Context, job *rayv1.RayJob) {
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   wfv1.NodePending,
			Message: string(job.Status.JobDeploymentStatus),
			Outputs: buildRayJobOutputs(job),
		},
		Requeue: &metav1.Duration{Duration: 10 * time.Second},
	})
}

// ResponseRayJob responds based on the current RayJob status.
func (ct *RayClusterController) ResponseRayJob(ctx *gin.Context, job *rayv1.RayJob) {
	phase := getRayJobPhase(job.Status.JobStatus)

	var requeue *metav1.Duration
	if phase == wfv1.NodeRunning || phase == wfv1.NodePending {
		requeue = &metav1.Duration{Duration: 10 * time.Second}
	}

	message := job.Status.Message
	if message == "" {
		message = string(job.Status.JobDeploymentStatus)
	}

	klog.Infof("### RayJob %v status: jobStatus=%v deploymentStatus=%v", job.Name, job.Status.JobStatus, job.Status.JobDeploymentStatus)
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   phase,
			Message: message,
			Outputs: buildRayJobOutputs(job),
		},
		Requeue: requeue,
	})
}

func buildRayJobOutputs(job *rayv1.RayJob) *wfv1.Outputs {
	if job == nil {
		return nil
	}
	parameters := []wfv1.Parameter{
		{Name: "ray-job-status", Value: wfv1.AnyStringPtr(string(job.Status.JobStatus))},
		{Name: "ray-job-deployment-status", Value: wfv1.AnyStringPtr(string(job.Status.JobDeploymentStatus))},
		{Name: "ray-job-message", Value: wfv1.AnyStringPtr(job.Status.Message)},
		{Name: "ray-job-id", Value: wfv1.AnyStringPtr(job.Status.JobId)},
	}
	if job.Status.StartTime != nil {
		parameters = append(parameters, wfv1.Parameter{
			Name:  "ray-job-start-time",
			Value: wfv1.AnyStringPtr(job.Status.StartTime.String()),
		})
	}
	if job.Status.EndTime != nil {
		parameters = append(parameters, wfv1.Parameter{
			Name:  "ray-job-end-time",
			Value: wfv1.AnyStringPtr(job.Status.EndTime.String()),
		})
	}
	if job.Spec.RuntimeEnvYAML != "" {
		parameters = append(parameters, wfv1.Parameter{
			Name:  "ray-job-runtime-env-yaml",
			Value: wfv1.AnyStringPtr(job.Spec.RuntimeEnvYAML),
		})
	}
	return &wfv1.Outputs{Parameters: parameters}
}

// getRayJobPhase converts RayJob JobStatus to Argo Workflow NodePhase.
func getRayJobPhase(jobStatus rayv1.JobStatus) wfv1.NodePhase {
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
		klog.Infof("Unknown RayJob status: %v, treating as pending", jobStatus)
		return wfv1.NodePending
	}
}
