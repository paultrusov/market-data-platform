CLUSTER ?= mdp
NS      ?= mdp
TAG     ?= dev
SOURCE  ?= coinbase

.PHONY: up cluster images load deploy wait status logs down drill-node drill-pod test fmt

## bring the whole platform up from nothing
up: cluster images load deploy wait status

cluster:
	@kind get clusters 2>/dev/null | grep -qx $(CLUSTER) || kind create cluster --config kind-config.yaml
	@kubectl config use-context kind-$(CLUSTER) >/dev/null

images:
	docker build -q -t mdp-ingest:$(TAG) ingest
	docker build -q -t mdp-replay:$(TAG) replay

# kind has no registry, so images are side-loaded onto the nodes directly.
load:
	kind load docker-image mdp-ingest:$(TAG) mdp-replay:$(TAG) --name $(CLUSTER)

deploy:
	cd infra && terraform init -input=false -no-color >/dev/null && \
	  terraform apply -auto-approve -no-color -var image_tag=$(TAG) -var ingest_source=$(SOURCE)

wait:
	kubectl -n $(NS) rollout status statefulset/postgres  --timeout=300s
	kubectl -n $(NS) rollout status deployment/ingest     --timeout=300s
	kubectl -n $(NS) rollout status deployment/replay     --timeout=300s
	kubectl -n $(NS) rollout status deployment/prometheus --timeout=300s

status:
	@kubectl -n $(NS) get pods -o wide
	@echo
	@curl -sf http://localhost:30080/stats | python3 -m json.tool || echo "replay API not answering yet"

logs:
	kubectl -n $(NS) logs -l app=ingest --tail=30 --prefix

drill-node:
	python3 drills/node_kill.py

drill-pod:
	python3 drills/pod_kill.py

test:
	python3 -m pytest tests -q

fmt:
	cd infra && terraform fmt

down:
	-cd infra && terraform destroy -auto-approve -no-color
	-kind delete cluster --name $(CLUSTER)
