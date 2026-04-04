package raycluster

import (
	"encoding/json"
	"sort"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// BuildOutputs builds the Argo workflow output parameters from a RayCluster's status.
func BuildOutputs(cluster *rayv1.RayCluster) *wfv1.Outputs {
	if cluster == nil {
		return nil
	}

	host := resolveHost(cluster)

	parameters := []wfv1.Parameter{
		{Name: "ray-cluster-state", Value: wfv1.AnyStringPtr(string(cluster.Status.State))},
		{Name: "ray-cluster-reason", Value: wfv1.AnyStringPtr(cluster.Status.Reason)},
		{Name: "ray-head-pod-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodName)},
		{Name: "ray-head-pod-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.PodIP)},
		{Name: "ray-head-service-name", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceName)},
		{Name: "ray-head-service-ip", Value: wfv1.AnyStringPtr(cluster.Status.Head.ServiceIP)},
	}

	if cluster.Status.Head.ServiceName != "" && cluster.Namespace != "" {
		fqdn := cluster.Status.Head.ServiceName + "." + cluster.Namespace + ".svc.cluster.local"
		parameters = append(parameters, wfv1.Parameter{
			Name:  "ray-head-service-fqdn",
			Value: wfv1.AnyStringPtr(fqdn),
		})
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

	// Construct commonly-used addresses from host + endpoint ports.
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

	// Append all endpoints as individual parameters (sorted for determinism).
	if len(cluster.Status.Endpoints) > 0 {
		keys := make([]string, 0, len(cluster.Status.Endpoints))
		for key := range cluster.Status.Endpoints {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		replacer := strings.NewReplacer("/", "-", ".", "-", "_", "-")
		for _, key := range keys {
			sanitizedKey := replacer.Replace(strings.ToLower(key))
			parameters = append(parameters, wfv1.Parameter{
				Name:  "ray-endpoint-" + sanitizedKey,
				Value: wfv1.AnyStringPtr(cluster.Status.Endpoints[key]),
			})
		}
	}

	return &wfv1.Outputs{Parameters: parameters}
}

// BuildAnnotations returns the key/value pairs to be written to the Workflow annotations
// when a RayCluster becomes ready.
func BuildAnnotations(cluster *rayv1.RayCluster) map[string]string {
	host := resolveHost(cluster)

	ann := map[string]string{
		"ray.io/cluster-state":     string(cluster.Status.State),
		"ray.io/head-service-fqdn": host,
		"ray.io/head-pod-ip":       cluster.Status.Head.PodIP,
		"ray.io/head-service-ip":   cluster.Status.Head.ServiceIP,
		"ray.io/head-service-name": cluster.Status.Head.ServiceName,
	}

	if host != "" {
		if port, ok := cluster.Status.Endpoints["client"]; ok && port != "" {
			ann["ray.io/address"] = "ray://" + host + ":" + port
		}
		if port, ok := cluster.Status.Endpoints["dashboard"]; ok && port != "" {
			ann["ray.io/dashboard-url"] = "http://" + host + ":" + port
		}
		if port, ok := cluster.Status.Endpoints["gcs-server"]; ok && port != "" {
			ann["ray.io/gcs-address"] = host + ":" + port
		}
	}

	if len(cluster.Status.Endpoints) > 0 {
		if data, err := json.Marshal(cluster.Status.Endpoints); err == nil {
			ann["ray.io/endpoints"] = string(data)
		}
	}

	return ann
}

// resolveHost returns the best available host identifier for the cluster head:
// FQDN > ServiceIP > PodIP.
func resolveHost(cluster *rayv1.RayCluster) string {
	if cluster.Status.Head.ServiceName != "" && cluster.Namespace != "" {
		return cluster.Status.Head.ServiceName + "." + cluster.Namespace + ".svc.cluster.local"
	}
	if cluster.Status.Head.ServiceIP != "" {
		return cluster.Status.Head.ServiceIP
	}
	return cluster.Status.Head.PodIP
}
