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

// Execute handles both RayCluster and RayJob requests
func (ct *RayClusterController) Execute(ctx *gin.Context) {
	c := &executorplugins.ExecuteTemplateArgs{}
	err := ctx.BindJSON(&c)
	if err != nil {
		klog.Error(err)
		return
	}

	pluginJson, _ := c.Template.Plugin.MarshalJSON()
	klog.Info("Receive plugin: ", string(pluginJson))

	// Check if it's a RayJob by looking for "kind: RayJob" in the JSON
	if strings.Contains(string(pluginJson), "kind") && strings.Contains(string(pluginJson), "RayJob") {
		// Check it's not RayCluster
		if !strings.Contains(string(pluginJson), "RayCluster") {
			klog.Info("### found ray job in plugin")
			ct.handleRayJob(ctx, c)
			return
		}
	}

	// Check if it's a RayCluster
	if strings.Contains(string(pluginJson), "RayCluster") {
		klog.Info("### found ray cluster in plugin")
		ct.handleRayCluster(ctx, c)
		return
	}

	klog.Info("### no ray cluster or job in plugin")
	ct.Response200(ctx)
}

func (ct *RayClusterController) handleRayJob(ctx *gin.Context, c *executorplugins.ExecuteTemplateArgs) {
	inputBody := &RayJobPluginBody{
		RayJob: &rayv1.RayJob{},
	}

	pluginJson, _ := c.Template.Plugin.MarshalJSON()
	err := json.Unmarshal(pluginJson, &inputBody)
	if err != nil {
		klog.Error(err)
		ct.Response404(ctx)
		return
	}

	job := inputBody.RayJob

	if job.Name == "" {
		job.ObjectMeta.Name = c.Workflow.ObjectMeta.Name
	}

	if job.ObjectMeta.Namespace == "" {
		job.Namespace = "default"
	}

	// 1. query job exists
	existsJob, err := ct.RayClient.RayV1().RayJobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		existsJob = nil
	}

	// 2. found and return
	if existsJob != nil {
		klog.Info("# found exists Ray Job: ", job.Name, "returning Status...", existsJob.Status)
		ct.ResponseRayJob(ctx, existsJob)
		return
	}

	// 3. Label keys with workflow Name
	InjectRayJobWithWorkflowName(job, c.Workflow.ObjectMeta.Name)

	newJob, err := ct.RayClient.RayV1().RayJobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		klog.Error("### " + err.Error())
		ct.ResponseMsg(ctx, wfv1.NodeFailed, err.Error())
		return
	}

	ct.ResponseJobCreated(ctx, newJob)
}

func (ct *RayClusterController) handleRayCluster(ctx *gin.Context, c *executorplugins.ExecuteTemplateArgs) {
	inputBody := &RayPluginBody{
		RayCluster: &rayv1.RayCluster{},
	}

	pluginJson, _ := c.Template.Plugin.MarshalJSON()
	err := json.Unmarshal(pluginJson, &inputBody)
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

	host := cluster.Status.Head.ServiceIP
	if host == "" {
		host = cluster.Status.Head.PodIP
	}

	parameters := []wfv1.Parameter{
		{Name: "ray-cluster-state", Value: wfv1.AnyStringPtr(string(cluster.Status.State))},
		{Name: "ray-cluster-reason", Value: wfv1.AnyStringPtr(cluster.Status.Reason)},
		{Name: "ray-head-pod-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodName)},
		{Name: "ray-head-pod-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodIP)},
		{Name: "ray-head-service-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceName)},
		{Name: "ray-head-service-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceIP)},
	}

	// FQDN for in-cluster access (more stable than IP)
	if cluster.Status.Head.ServiceName != "" && cluster.Namespace != "" {
		fqdn := cluster.Status.Head.ServiceName + "." + cluster.Namespace + ".svc.cluster.local"
		parameters = append(parameters, wfv1.Parameter{
			Name:  "ray-head-service-fqdn",
			Value: wfv1.AnyStringPtr(fqdn),
		})
		// Prefer FQDN as host for URL construction
		host = fqdn
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

	// Construct commonly used addresses from host + endpoint ports
	// KubeRay endpoint keys: "client" (10001), "dashboard" (8265), "gcs-server" (6379), "serve" (8000)
	if host != "" && len(cluster.Status.Endpoints) > 0 {
		if port, ok := cluster.Status.Endpoints["client"]; ok && port != "" {
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-address",
				Value: wfv1.AnyStringPtr("ray://" + host + ":" + port),
			})
		}
		if port, ok := cluster.Status.Endpoints["dashboard"]; ok && port != "" {
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-dashboard-url",
				Value: wfv1.AnyStringPtr("http://" + host + ":" + port),
			})
		}
		if port, ok := cluster.Status.Endpoints["gcs-server"]; ok && port != "" {
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-gcs-address",
				Value: wfv1.AnyStringPtr(host + ":" + port),
			})
		}
		if port, ok := cluster.Status.Endpoints["serve"]; ok && port != "" {
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-serve-url",
				Value: wfv1.AnyStringPtr("http://" + host + ":" + port),
			})
		}
	}

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
