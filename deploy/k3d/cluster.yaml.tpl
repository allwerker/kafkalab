# k3d cluster definition. Rendered by `make up` (envsubst) from tools/versions.env and env vars.
apiVersion: k3d.io/v1alpha5
kind: Simple
metadata:
  name: ${CLUSTER_NAME}
servers: 1
agents: 0
image: ${K3S_IMAGE}
kubeAPI:
  host: "127.0.0.1"
  hostIP: "127.0.0.1"
  hostPort: "${API_PORT}"
ports:
  # Single ingress entry point, bound to loopback only: a chaos panel must not be reachable from the LAN.
  - port: 127.0.0.1:${LB_PORT}:80
    nodeFilters: [loadbalancer]
registries:
  create:
    name: ${CLUSTER_NAME}-registry.localhost
    host: "127.0.0.1"
    hostPort: "${REGISTRY_PORT}"
volumes:
  # Persistent volumes survive `make down` / `make up`; `make nuke` deletes them.
  - volume: ${DATA_DIR}:/var/lib/rancher/k3s/storage
    nodeFilters: [server:0]
options:
  k3d:
    wait: true
    timeout: "180s"
  k3s:
    extraArgs:
      - arg: --disable=metrics-server   # VictoriaMetrics stack provides kube-state-metrics + kubelet scraping
        nodeFilters: [server:*]
      - arg: --kubelet-arg=max-pods=150
        nodeFilters: [server:*]
      - arg: --kube-controller-manager-arg=bind-address=0.0.0.0
        nodeFilters: [server:*]
      - arg: --kube-scheduler-arg=bind-address=0.0.0.0
        nodeFilters: [server:*]
  kubeconfig:
    updateDefaultKubeconfig: false
    switchCurrentContext: false
