# Azure DevOps Delivery Dashboard

A Kubernetes-ready real-time dashboard showing builds in progress, queued builds, completed builds, and deployments waiting for approval.

## Run locally

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

## Build and deploy

```bash
docker build -t your-registry/monitoring-deployments:1.0.0 .
docker push your-registry/monitoring-deployments:1.0.0
kubectl apply -f k8s/namespace.yaml
kubectl create secret generic monitoring-deployments-secret -n monitoring-deployments --from-literal=AZDO_PAT='YOUR_PAT'
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/ingress.yaml
```

Update `k8s/deployment.yaml` with your image, Azure DevOps organization, and project. The PAT needs Build Read and Release Read permissions. Store it only in a Kubernetes Secret.

For testing:

```bash
kubectl port-forward -n monitoring-deployments svc/monitoring-deployments 8080:80
```

Open `http://localhost:8080`.
