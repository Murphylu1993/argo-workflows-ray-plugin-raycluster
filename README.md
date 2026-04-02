# argo workflows ray plugin


A plugin lets Argo Workflows orchestrate Ray clusters.


## Why argo-workflows-ray-plugin

* Submit tasks using non-string methods. More flexibly control and observe the status of Ray clusters.

* Save costs. In scenarios where a large number of Ray clusters are orchestrated, there is no need to generate an equal number of resource pods.

## Getting Started

1. Enable Plugin capability for controller
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workflow-controller
spec:
  template:
    spec:
      containers:
        - name: workflow-controller
          env:
            - name: ARGO_EXECUTOR_PLUGINS
              value: "true"
```
2. Build argo-ray-plugin image

```
git clone https://github.com/shuangkun/argo-workflows-ray-plugin.git
cd argo-workflows-ray-plugin
docker build -t argo-ray-plugin:v1 .
```
3. Deploy argo-ray-plugin
```
kubectl apply -f ray-executor-plugin-configmap.yaml
```

4. Permission to create RayCluster CRD
```
kubctl apply -f install/role-secret.yaml
```

4. Submit Ray clusters
```
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: ray-distributed-demo-
  namespace: argo
spec:
  entrypoint: ray-demo
  templates:
    - name: ray-demo
      plugin:
        ray:
          # RayCluster definition (Ray Operator must be installed in advance)
          apiVersion: ray.io/v1
          kind: RayCluster
          metadata:
            name: ray-example
            namespace: argo
          spec:
            rayVersion: "2.9.3"
            headGroupSpec:
              rayStartParams:
                dashboard-host: '0.0.0.0'
              template:
                spec:
                  containers:
                    - name: ray-head
                      image: rayproject/ray:2.9.3
                      ports:
                        - containerPort: 6379
                        - containerPort: 8265
                      resources:
                        limits:
                          cpu: 2
                          memory: 4Gi
            workerGroupSpecs:
              - replicas: 2
                minReplicas: 1
                maxReplicas: 3
                groupName: worker
                rayStartParams: {}
                template:
                  spec:
                    containers:
                      - name: ray-worker
                        image: rayproject/ray:2.9.3
                        resources:
                          limits:
                            cpu: 4
                            memory: 8Gi
```