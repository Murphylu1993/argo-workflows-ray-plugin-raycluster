package rayjob

import (
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// BuildOutputs builds the Argo workflow output parameters from a RayJob's status.
func BuildOutputs(job *rayv1.RayJob) *wfv1.Outputs {
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

// BuildAnnotations returns key/value pairs to be written to the Workflow annotations
// when a RayJob reaches a terminal state.
func BuildAnnotations(job *rayv1.RayJob) map[string]string {
	ann := map[string]string{
		"ray.io/job-id":                job.Status.JobId,
		"ray.io/job-status":            string(job.Status.JobStatus),
		"ray.io/job-deployment-status": string(job.Status.JobDeploymentStatus),
		"ray.io/job-message":           job.Status.Message,
	}
	if job.Status.StartTime != nil {
		ann["ray.io/job-start-time"] = job.Status.StartTime.String()
	}
	if job.Status.EndTime != nil {
		ann["ray.io/job-end-time"] = job.Status.EndTime.String()
	}
	return ann
}
