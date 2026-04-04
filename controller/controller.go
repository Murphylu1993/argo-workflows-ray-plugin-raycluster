package controller

import (
	"net/http"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	wfversioned "github.com/argoproj/argo-workflows/v3/pkg/client/clientset/versioned"
	executorplugins "github.com/argoproj/argo-workflows/v3/pkg/plugins/executor"
	"github.com/gin-gonic/gin"
	rayversioned "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	"k8s.io/klog/v2"

	"argo-workflows-ray-plugin/controller/raycluster"
	"argo-workflows-ray-plugin/controller/rayjob"
)

// RayController handles Argo Workflow plugin requests for RayCluster and RayJob resources.
type RayController struct {
	RayClient  *rayversioned.Clientset
	// ArgoClient is used to patch Workflow annotations with cluster/job info.
	ArgoClient wfversioned.Interface
}

// Execute is the Gin handler for POST /api/v1/template.execute.
// It parses the incoming plugin body and dispatches to the appropriate resource handler.
func (ct *RayController) Execute(ctx *gin.Context) {
	args := &executorplugins.ExecuteTemplateArgs{}
	if err := ctx.BindJSON(args); err != nil {
		klog.Error("Execute: failed to bind JSON:", err)
		return
	}

	pluginJSON, _ := args.Template.Plugin.MarshalJSON()
	klog.Info("Execute: received plugin body: ", string(pluginJSON))
	body := string(pluginJSON)

	switch {
	case containsKind(body, "RayJob"):
		klog.Info("Execute: dispatching to RayJob handler")
		rayjob.Handle(ctx, args, ct.RayClient, ct.ArgoClient)

	case containsKind(body, "RayCluster"):
		klog.Info("Execute: dispatching to RayCluster handler")
		raycluster.Handle(ctx, args, ct.RayClient, ct.ArgoClient)

	default:
		klog.Info("Execute: no recognized Ray resource kind in plugin body")
		ctx.JSON(http.StatusOK, &executorplugins.ExecuteTemplateReply{
			Node: &wfv1.NodeResult{
				Phase:   wfv1.NodeSucceeded,
				Message: "no Ray resource found in plugin body",
			},
		})
	}
}

// containsKind returns true when the JSON body contains a "kind" field with the given value.
// It uses a simple string match to avoid a full unmarshal for routing purposes.
func containsKind(body, kind string) bool {
	return strings.Contains(body, `"kind"`) && strings.Contains(body, kind)
}
