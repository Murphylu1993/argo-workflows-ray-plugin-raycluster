package controller

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	executorplugins "github.com/argoproj/argo-workflows/v3/pkg/plugins/executor"
	"github.com/gin-gonic/gin"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayversioned "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	LabelKeyWorkflow string = "workflows.argoproj.io/workflow"
)

type RayClusterController struct {
	RayClient *rayversioned.Clientset
}

type RayPluginBody struct {
	RayCluster *rayv1.RayCluster `json:"ray"`
}

func (ct *RayClusterController) ExecuteRayCluster(ctx *gin.Context) {
	c := &executorplugins.ExecuteTemplateArgs{}
	err := ctx.BindJSON(&c)
	if err != nil {
		klog.Error(err)
		return
	}

	inputBody := &RayPluginBody{
		RayCluster: &rayv1.RayCluster{},
	}

	pluginJson, _ := c.Template.Plugin.MarshalJSON()
	klog.Info("Receive: ", string(pluginJson))
	if !strings.Contains(string(pluginJson), "RayCluster") {
		klog.Info("### no ray cluster in plugin")
		ct.Response200(ctx)
		return
	} else {
		klog.Info("### found ray cluster in plugin")
	}
	err = json.Unmarshal(pluginJson, &inputBody)
	if err != nil {
		klog.Error(err)
		ct.Response404(ctx)
		return
	}

	cluster := inputBody.RayCluster

	if cluster.Name == "" {
		cluster.ObjectMeta.Name = c.Workflow.ObjectMeta.Name
	}

	if cluster.ObjectMeta.Namespace == "" {
		cluster.Namespace = "default"
	}

	var exists = false

	// 1. query cluster exists
	existsCluster, err := ct.RayClient.RayV1().RayClusters(cluster.Namespace).Get(ctx, cluster.Name, metav1.GetOptions{})
	if err != nil {
		exists = false
	} else {
		exists = true
	}
	// 2. found and return
	if exists {
		klog.Info("# found exists Ray Cluster: ", cluster.Name, "returning Status...", existsCluster.Status)
		ct.ResponseRayCluster(ctx, existsCluster)
		return
	}

	// 3.Label keys with workflow Name
	InjectRayClusterWithWorkflowName(cluster, c.Workflow.ObjectMeta.Name)

	newCluster, err := ct.RayClient.RayV1().RayClusters(cluster.Namespace).Create(ctx, cluster, metav1.CreateOptions{})
	if err != nil {
		klog.Error("### " + err.Error())
		ct.ResponseMsg(ctx, wfv1.NodeFailed, err.Error())
		return
	}

	ct.ResponseCreated(ctx, newCluster)

}

func (ct *RayClusterController) ResponseCreated(ctx *gin.Context, cluster *rayv1.RayCluster) {
	message := cluster.Status.Reason
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   wfv1.NodePending,
			Message: message,
			Outputs: buildRayClusterOutputs(cluster),
		},
		Requeue: &metav1.Duration{
			Duration: 10 * time.Second,
		},
	})
}

func (ct *RayClusterController) ResponseMsg(ctx *gin.Context, status wfv1.NodePhase, msg string) {
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:   status,
			Message: msg,
			Outputs: nil,
		},
	})
}

func (ct *RayClusterController) ResponseRayCluster(ctx *gin.Context, cluster *rayv1.RayCluster) {
	var status wfv1.NodePhase
	switch cluster.Status.State {
	case rayv1.Ready:
		status = wfv1.NodeSucceeded
	case rayv1.Failed:
		status = wfv1.NodeFailed
	case rayv1.Suspended:
		status = wfv1.NodeSucceeded
	default:
		if isRayClusterConditionTrue(cluster, rayv1.HeadPodReady) || isRayClusterConditionTrue(cluster, rayv1.RayClusterProvisioned) {
			status = wfv1.NodeSucceeded
		} else {
			status = wfv1.NodePending
		}
	}

	var requeue *metav1.Duration
	if status == wfv1.NodeRunning || status == wfv1.NodePending {
		requeue = &metav1.Duration{
			Duration: 10 * time.Second,
		}
	} else {
		requeue = nil
	}

	succeed := int32(0)
	total := 1
	if cluster.Status.Head.PodName != "" {
		if status == wfv1.NodeSucceeded {
			succeed = 1
		}
	}
	progress, _ := wfv1.NewProgress(int64(succeed), int64(total))
	klog.Infof("### Cluster %v status: %v", cluster.Name, status)
	message := cluster.Status.Reason
	if message == "" {
		message = string(cluster.Status.State)
	}
	ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
		Node: &wfv1.NodeResult{
			Phase:    status,
			Message:  message,
			Outputs:  buildRayClusterOutputs(cluster),
			Progress: progress,
		},
		Requeue: requeue,
	})
}

func buildRayClusterOutputs(cluster *rayv1.RayCluster) *wfv1.Outputs {
	if cluster == nil {
		return nil
	}

	parameters := []wfv1.Parameter{
		{Name: "ray-cluster-state", Value: wfv1.AnyStringPtr(string(cluster.Status.State))},
		{Name: "ray-cluster-reason", Value: wfv1.AnyStringPtr(cluster.Status.Reason)},
		{Name: "ray-head-pod-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodName)},
		{Name: "ray-head-pod-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodIP)},
		{Name: "ray-head-service-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceName)},
		{Name: "ray-head-service-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceIP)},
	}

	endpointsJSON := "{}"
	if len(cluster.Status.Endpoints) > 0 {
		if data, err := json.Marshal(cluster.Status.Endpoints); err == nil {
			endpointsJSON = string(data)
		}
	}
	parameters = append(parameters, wfv1.Parameter{
		Name:  "ray-endpoints-json",
		Value: wfv1.AnyStringPtr(endpointsJSON),
	})

	if len(cluster.Status.Endpoints) > 0 {
		keys := make([]string, 0, len(cluster.Status.Endpoints))
		for key := range cluster.Status.Endpoints {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			sanitizedKey := strings.NewReplacer("/", "-", ".", "-", "_", "-").Replace(strings.ToLower(key))
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-endpoint-" + sanitizedKey,
				Value: wfv1.AnyStringPtr(cluster.Status.Endpoints[key]),
			})
		}
	}

	return &wfv1.Outputs{Parameters: parameters}
}

func isRayClusterConditionTrue(cluster *rayv1.RayCluster, condType rayv1.RayClusterConditionType) bool {
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == string(condType) && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (ct *RayClusterController) Response404(ctx *gin.Context) {
	ctx.AbortWithStatus(http.StatusNotFound)
}

func (ct *RayClusterController) Response200(ctx *gin.Context) {
	ctx.AbortWithStatus(http.StatusOK)
}

func InjectRayClusterWithWorkflowName(cluster *rayv1.RayCluster, workflowName string) {
	headGroupSpec := cluster.Spec.HeadGroupSpec
	if headGroupSpec.Template.ObjectMeta.Labels != nil {
		headGroupSpec.Template.ObjectMeta.Labels[LabelKeyWorkflow] = workflowName
	} else {
		headGroupSpec.Template.ObjectMeta.Labels = map[string]string{
			LabelKeyWorkflow: workflowName,
		}
	}

	workerGroupSpecs := make([]rayv1.WorkerGroupSpec, len(cluster.Spec.WorkerGroupSpecs))
	copy(workerGroupSpecs, cluster.Spec.WorkerGroupSpecs)

	for i := range workerGroupSpecs {
		labels := workerGroupSpecs[i].Template.ObjectMeta.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		labels[LabelKeyWorkflow] = workflowName
		workerGroupSpecs[i].Template.ObjectMeta.Labels = labels
	}

	cluster.Spec.HeadGroupSpec = headGroupSpec
	cluster.Spec.WorkerGroupSpecs = workerGroupSpecs
}
