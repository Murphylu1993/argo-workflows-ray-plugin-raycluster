package rayjob

import (
	"encoding/json"
	"net/http"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	wfversioned "github.com/argoproj/argo-workflows/v3/pkg/client/clientset/versioned"
	executorplugins "github.com/argoproj/argo-workflows/v3/pkg/plugins/executor"
	"github.com/gin-gonic/gin"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayversioned "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"argo-workflows-ray-plugin/controller/common"
)

// PluginBody is the plugin JSON body for RayJob templates.
type PluginBody struct {
	RayJob *rayv1.RayJob `json:"ray"`
}

// Handle is the entry point for processing a RayJob plugin request.
func Handle(
	ctx *gin.Context,
	args *executorplugins.ExecuteTemplateArgs,
	rayClient *rayversioned.Clientset,
	argoClient wfversioned.Interface,
) {
	pluginJSON, _ := args.Template.Plugin.MarshalJSON()

	body := &PluginBody{RayJob: &rayv1.RayJob{}}
	if err := json.Unmarshal(pluginJSON, body); err != nil {
		klog.Error("rayjob: failed to unmarshal plugin body:", err)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	job := body.RayJob
	wfMeta := args.Workflow.ObjectMeta
	// Always co-locate the RayJob in the same namespace as the Argo Workflow.
	common.SetDefaultMeta(&job.ObjectMeta, wfMeta.Name, wfMeta.Namespace)

	// 1. Query whether the job already exists.
	existing, err := rayClient.RayV1().RayJobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		existing = nil
	}

	// 2. Found: patch workflow annotations on terminal state, then return current status.
	if existing != nil {
		klog.Infof("rayjob: found existing RayJob %s/%s, returning status: jobStatus=%v deploymentStatus=%v",
			existing.Namespace, existing.Name, existing.Status.JobStatus, existing.Status.JobDeploymentStatus)
		phase := GetPhase(existing.Status.JobStatus)
		if phase == wfv1.NodeSucceeded || phase == wfv1.NodeFailed {
			common.PatchWorkflowAnnotations(ctx, argoClient,
				wfMeta.Namespace, wfMeta.Name,
				BuildAnnotations(existing))
		}
		respond(ctx, existing)
		return
	}

	// 3. Not found: inject labels, set owner reference, then create.
	injectLabels(job, wfMeta.Name)
	common.AttachWorkflowOwnerReference(
		&job.ObjectMeta,
		wfMeta.Name,
		wfMeta.Namespace,
		wfMeta.Uid,
	)

	created, err := rayClient.RayV1().RayJobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("rayjob: failed to create RayJob %s/%s: %v", job.Namespace, job.Name, err)
		ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
			Node: &wfv1.NodeResult{
				Phase:   wfv1.NodeFailed,
				Message: err.Error(),
			},
		})
		return
	}

	klog.Infof("rayjob: created RayJob %s/%s", created.Namespace, created.Name)
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   wfv1.NodePending,
			Message: string(created.Status.JobDeploymentStatus),
			Outputs: BuildOutputs(created),
		},
		Requeue: &metav1.Duration{Duration: 10 * time.Second},
	})
}

// respond sends an ExecuteTemplateReply reflecting the current job state.
func respond(ctx *gin.Context, job *rayv1.RayJob) {
	phase := GetPhase(job.Status.JobStatus)

	var requeue *metav1.Duration
	if phase == wfv1.NodeRunning || phase == wfv1.NodePending {
		requeue = &metav1.Duration{Duration: 10 * time.Second}
	}

	message := job.Status.Message
	if message == "" {
		message = string(job.Status.JobDeploymentStatus)
	}

	klog.Infof("rayjob: RayJob %s/%s phase=%s jobStatus=%v deploymentStatus=%v",
		job.Namespace, job.Name, phase, job.Status.JobStatus, job.Status.JobDeploymentStatus)
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   phase,
			Message: message,
			Outputs: BuildOutputs(job),
		},
		Requeue: requeue,
	})
}

// injectLabels adds the workflow name label to the RayJob and its embedded cluster spec.
func injectLabels(job *rayv1.RayJob, workflowName string) {
	if job.ObjectMeta.Labels == nil {
		job.ObjectMeta.Labels = map[string]string{}
	}
	job.ObjectMeta.Labels[common.LabelKeyWorkflow] = workflowName

	if job.Spec.RayClusterSpec != nil {
		headLabels := job.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta.Labels
		if headLabels == nil {
			headLabels = map[string]string{}
		}
		headLabels[common.LabelKeyWorkflow] = workflowName
		job.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta.Labels = headLabels
	}
}
