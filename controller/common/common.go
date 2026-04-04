package common

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"
	wfversioned "github.com/argoproj/argo-workflows/v3/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const (
	// LabelKeyWorkflow is the label key used to associate Ray resources with an Argo Workflow.
	LabelKeyWorkflow string = "workflows.argoproj.io/workflow"
)

// SetDefaultMeta fills in Name and Namespace on objMeta when they are empty.
// workflowName is used when objMeta.Name is empty.
// workflowNamespace is always used as the namespace so that Ray resources are
// co-located in the same namespace as the owning Argo Workflow.
func SetDefaultMeta(objMeta *metav1.ObjectMeta, workflowName, workflowNamespace string) {
	if objMeta.Name == "" {
		objMeta.Name = workflowName
	}
	// Always align the Ray resource namespace with the workflow namespace so
	// that owner-references and RBAC stay within a single namespace.
	if workflowNamespace != "" {
		if objMeta.Namespace != "" && objMeta.Namespace != workflowNamespace {
			klog.Warningf("SetDefaultMeta: overriding Ray resource namespace %q with workflow namespace %q",
				objMeta.Namespace, workflowNamespace)
		}
		objMeta.Namespace = workflowNamespace
	} else if objMeta.Namespace == "" {
		objMeta.Namespace = "default"
	}
}

// AttachWorkflowOwnerReference sets the Workflow as an owner of the resource.
// This enables Kubernetes garbage collection to delete Ray resources together with their Workflow.
// Cross-namespace owner references are invalid in Kubernetes and are silently skipped.
func AttachWorkflowOwnerReference(objMeta *metav1.ObjectMeta, workflowName, workflowNamespace, workflowUID string) {
	if objMeta == nil || workflowUID == "" {
		return
	}
	if objMeta.Namespace != "" && workflowNamespace != "" && objMeta.Namespace != workflowNamespace {
		// Cross-namespace owner references are invalid.
		return
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: "argoproj.io/v1alpha1",
		Kind:       "Workflow",
		Name:       workflowName,
		UID:        types.UID(workflowUID),
	}

	for i := range objMeta.OwnerReferences {
		if objMeta.OwnerReferences[i].UID == ownerRef.UID {
			objMeta.OwnerReferences[i] = ownerRef
			return
		}
	}
	objMeta.OwnerReferences = append(objMeta.OwnerReferences, ownerRef)
}

// PatchWorkflowAnnotations merges the given annotations into the Workflow resource via a JSON merge-patch.
// It uses the typed Argo Workflows client instead of the generic dynamic client.
func PatchWorkflowAnnotations(ctx *gin.Context, argoClient wfversioned.Interface, namespace, name string, annotations map[string]string) {
	if argoClient == nil || namespace == "" || name == "" || len(annotations) == 0 {
		return
	}

	annJSON, err := json.Marshal(annotations)
	if err != nil {
		klog.Errorf("PatchWorkflowAnnotations: marshal error: %v", err)
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":%s}}`, string(annJSON))

	_, err = argoClient.ArgoprojV1alpha1().Workflows(namespace).Patch(
		context.Background(),
		name,
		types.MergePatchType,
		[]byte(patch),
		metav1.PatchOptions{},
	)
	if err != nil {
		klog.Errorf("PatchWorkflowAnnotations: patch workflow %s/%s failed: %v", namespace, name, err)
		return
	}
	klog.Infof("PatchWorkflowAnnotations: patched workflow %s/%s with %s", namespace, name, patch)
}
