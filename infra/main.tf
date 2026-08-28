# Everything that runs in the cluster is declared here. The cluster itself is three
# lines of kind config; Terraform owns what is deployed onto it, so `terraform apply`
# is the only way state changes and `terraform destroy` leaves nothing behind.

terraform {
  required_version = ">= 1.6"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.35"
    }
  }
}

provider "kubernetes" {
  config_path    = var.kubeconfig
  config_context = var.kube_context
}

variable "kubeconfig" {
  type    = string
  default = "~/.kube/config"
}

variable "kube_context" {
  type    = string
  default = "kind-mdp"
}

variable "namespace" {
  type    = string
  default = "mdp"
}

variable "pg_password" {
  type      = string
  default   = "mdp-local-dev"
  sensitive = true
}

variable "ingest_source" {
  description = "coinbase for the live public feed, synthetic for a deterministic one"
  type        = string
  default     = "coinbase"
}

variable "symbols" {
  type    = string
  default = "BTC-USD,ETH-USD,SOL-USD"
}

variable "ingest_replicas" {
  description = "Two by default: the fleet survives losing one, because both write the same rows."
  type        = number
  default     = 2
}

variable "image_tag" {
  type    = string
  default = "dev"
}

locals {
  pg_host = "postgres.${var.namespace}.svc.cluster.local"
  pg_dsn  = "postgresql://mdp:${var.pg_password}@${local.pg_host}:5432/market"
}

resource "kubernetes_namespace" "mdp" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "db" {
  metadata {
    name      = "db"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  data = {
    POSTGRES_PASSWORD = var.pg_password
    PG_DSN            = local.pg_dsn
  }
}
