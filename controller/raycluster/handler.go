package raycluster

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

// PluginBody is the plugin JSON body for RayCluster templates.
type PluginBody struct {
	RayCluster *rayv1.RayCluster `json:"ray"`
}

// Handle is the entry point for processing a RayCluster plugin request.
func Handle(
	ctx *gin.Context,
	args *executorplugins.ExecuteTemplateArgs,
	rayClient *rayversioned.Clientset,
	argoClient wfversioned.Interface,
) {
	pluginJSON, _ := args.Template.Plugin.MarshalJSON()

	body := &PluginBody{RayCluster: &rayv1.RayCluster{}}
	if err := json.Unmarshal(pluginJSON, body); err != nil {
		klog.Error("raycluster: failed to unmarshal plugin body:", err)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	cluster := body.RayCluster
	wfMeta := args.Workflow.ObjectMeta
	// Always co-locate the RayCluster in the same namespace as the Argo Workflow.
	common.SetDefaultMeta(&cluster.ObjectMeta, wfMeta.Name, wfMeta.Namespace)

	// 1. Query whether the cluster already exists.
	existing, err := rayClient.RayV1().RayClusters(cluster.Namespace).Get(ctx, cluster.Name, metav1.GetOptions{})
	if err != nil {
		existing = nil
	}

	// 2. Found: optionally patch workflow annotations, then return current status.
	if existing != nil {
		klog.Infof("raycluster: found existing RayCluster %s/%s, returning status: %v",
			existing.Namespace, existing.Name, existing.Status.State)
		phase := GetPhase(existing)
		if phase == wfv1.NodeRunning || phase == wfv1.NodeSucceeded {
			common.PatchWorkflowAnnotations(ctx, argoClient,
				wfMeta.Namespace, wfMeta.Name,
				BuildAnnotations(existing))
		}
		respond(ctx, existing)
		return
	}

	// 3. Not found: inject labels, set owner reference, then create.
	injectLabels(cluster, wfMeta.Name)
	common.AttachWorkflowOwnerReference(
		&cluster.ObjectMeta,
		wfMeta.Name,
		wfMeta.Namespace,
		wfMeta.Uid,
	)

	created, err := rayClient.RayV1().RayClusters(cluster.Namespace).Create(ctx, cluster, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("raycluster: failed to create RayCluster %s/%s: %v", cluster.Namespace, cluster.Name, err)
		ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
			Node: &wfv1.NodeResult{
				Phase:   wfv1.NodeFailed,
				Message: err.Error(),
			},
		})
		return
	}

	klog.Infof("raycluster: created RayCluster %s/%s", created.Namespace, created.Name)
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   wfv1.NodePending,
			Message: created.Status.Reason,
			Outputs: BuildOutputs(created),
		},
		Requeue: &metav1.Duration{Duration: 10 * time.Second},
	})
}

// respond sends an ExecuteTemplateReply reflecting the current cluster state.
func respond(ctx *gin.Context, cluster *rayv1.RayCluster) {
	phase := GetPhase(cluster)

	var requeue *metav1.Duration
	if phase == wfv1.NodeRunning || phase == wfv1.NodePending {
		requeue = &metav1.Duration{Duration: 10 * time.Second}
	}

	succeed := int32(0)
	if cluster.Status.Head.PodName != "" && phase == wfv1.NodeSucceeded {
		succeed = 1
	}
	progress, _ := wfv1.NewProgress(int64(succeed), 1)

	message := cluster.Status.Reason
	if message == "" {
		message = string(cluster.Status.State)
	}

	klog.Infof("raycluster: RayCluster %s/%s phase=%s", cluster.Namespace, cluster.Name, phase)
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:    phase,
			Message:  message,
			Outputs:  BuildOutputs(cluster),
			Progress: progress,
		},
		Requeue: requeue,
	})
}

// injectLabels adds the workflow name label to all pod specs within the cluster.
func injectLabels(cluster *rayv1.RayCluster, workflowName string) {
	head := cluster.Spec.HeadGroupSpec
	if head.Template.ObjectMeta.Labels == nil {
		head.Template.ObjectMeta.Labels = map[string]string{}
	}
	head.Template.ObjectMeta.Labels[common.LabelKeyWorkflow] = workflowName

	workers := make([]rayv1.WorkerGroupSpec, len(cluster.Spec.WorkerGroupSpecs))
	copy(workers, cluster.Spec.WorkerGroupSpecs)
	for i := range workers {
		if workers[i].Template.ObjectMeta.Labels == nil {
			workers[i].Template.ObjectMeta.Labels = map[string]string{}
		}
		workers[i].Template.ObjectMeta.Labels[common.LabelKeyWorkflow] = workflowName
	}

	cluster.Spec.HeadGroupSpec = head
	cluster.Spec.WorkerGroupSpecs = workers
}
