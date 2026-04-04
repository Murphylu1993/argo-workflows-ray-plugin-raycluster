# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build -o argo-ray-plugin main.go
docker build -t argo-ray-plugin:latest .

# Run (default port 3028)
go run main.go
go run main.go --port 3029

# Test
go test ./...
go test -v -run TestNamePattern ./controller/raycluster/...

# Format / lint
go fmt ./...
golangci-lint run ./...
```

## Architecture

This is a Go HTTP service that implements the [Argo Executor Plugin Protocol](https://argoproj.github.io/argo-workflows/plugins/), allowing Argo Workflows to declaratively create and monitor KubeRay resources.

**Request flow:**

```
Argo Workflow Controller
  → Executor Plugin Sidecar
    → POST /api/v1/template.execute  (Gin, port 3028)
      → controller.go  (routes by "kind" string-match)
        → raycluster.Handle() or rayjob.Handle()
          → KubeRay clientset (create/get RayCluster or RayJob)
          → dynamic client (patch Workflow annotations)
```

**Handler pattern** (same for both resource types, in `controller/raycluster/` and `controller/rayjob/`):
1. Unmarshal plugin JSON into `PluginBody` (the `"ray"` key holds the full resource spec)
2. Check if resource already exists — if so, return current status (idempotent; Argo retries on transient errors)
3. If not found: inject workflow label, attach owner reference, create resource
4. Return `ExecuteTemplateReply` with phase + `Requeue: 10s` until terminal state

**Phase mapping** (`phase.go` in each package): translates KubeRay status → Argo `NodePhase`. For RayCluster, `Ready` maps to `NodeRunning` (not `NodeSucceeded`) because clusters are long-lived; `Suspended` maps to `NodeSucceeded`.

**`controller/common/common.go`** provides three shared utilities:
- `SetDefaultMeta` — fills empty name/namespace (defaults namespace to `"default"`)
- `AttachWorkflowOwnerReference` — links resource to workflow for Kubernetes GC; silently skips cross-namespace refs (invalid in Kubernetes)
- `PatchWorkflowAnnotations` — JSON merge-patches the Workflow resource with cluster/job metadata via dynamic client

**Kubernetes clients** (both initialized in `main.go` from in-cluster config or local kubeconfig):
- `rayversioned.Clientset` — typed KubeRay client for RayCluster/RayJob CRUD
- `dynamic.Interface` — used only for patching Workflow annotations

**Output parameters** (`outputs.go` in each package): extracts fields from resource status (head pod name/IP, service FQDN, endpoints, job ID, etc.) as Argo output parameters for downstream steps.
