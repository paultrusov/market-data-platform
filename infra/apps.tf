# The two stateless tiers. Both are plain Deployments with probes, resource bounds and
# a spread preference, and both are safe to lose a replica of.

locals {
  # A pod on a node that has gone away is evicted only after this many seconds. The
  # default is 300, which means a node failure costs five minutes of reduced capacity
  # before Kubernetes will even start a replacement. Ten seconds is the deliberate
  # trade: faster recovery, at the cost of reacting to a network blip as if it were a
  # dead node. For a feed consumer that is the right side of the trade, because a
  # spurious extra replica is harmless -- the writes are idempotent.
  fast_eviction_seconds = 10
}

resource "kubernetes_deployment" "ingest" {
  metadata {
    name      = "ingest"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    replicas = var.ingest_replicas
    selector {
      match_labels = { app = "ingest" }
    }
    template {
      metadata {
        labels = { app = "ingest" }
      }
      spec {
        # Spread across nodes, but never at the cost of scheduling. A soft
        # anti-affinity preference is one signal among many and the scheduler put both
        # replicas on one node anyway; a spread constraint states the intent directly.
        # ScheduleAnyway, not DoNotSchedule: with one of two workers down, a hard
        # constraint leaves the replacement pod pending at exactly the moment the
        # service needs it running.
        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "kubernetes.io/hostname"
          when_unsatisfiable = "ScheduleAnyway"
          label_selector {
            match_labels = { app = "ingest" }
          }
        }
        dynamic "toleration" {
          for_each = ["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"]
          content {
            key                = toleration.value
            operator           = "Exists"
            effect             = "NoExecute"
            toleration_seconds = local.fast_eviction_seconds
          }
        }
        container {
          name              = "ingest"
          image             = "mdp-ingest:${var.image_tag}"
          image_pull_policy = "IfNotPresent"
          port {
            name           = "http"
            container_port = 9100
          }
          env {
            name  = "SOURCE"
            value = var.ingest_source
          }
          env {
            name  = "SYMBOLS"
            value = var.symbols
          }
          env {
            name = "PG_DSN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db.metadata[0].name
                key  = "PG_DSN"
              }
            }
          }
          liveness_probe {
            http_get {
              path = "/healthz"
              port = 9100
            }
            initial_delay_seconds = 5
            period_seconds        = 10
            failure_threshold     = 3
          }
          readiness_probe {
            http_get {
              path = "/readyz"
              port = 9100
            }
            initial_delay_seconds = 3
            period_seconds        = 3
            failure_threshold     = 2
          }
          resources {
            requests = { cpu = "50m", memory = "96Mi" }
            limits   = { cpu = "500m", memory = "256Mi" }
          }
        }
      }
    }
  }
}

resource "kubernetes_deployment" "replay" {
  metadata {
    name      = "replay"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    replicas = 2
    selector {
      match_labels = { app = "replay" }
    }
    template {
      metadata {
        labels = { app = "replay" }
      }
      spec {
        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "kubernetes.io/hostname"
          when_unsatisfiable = "ScheduleAnyway"
          label_selector {
            match_labels = { app = "replay" }
          }
        }
        dynamic "toleration" {
          for_each = ["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"]
          content {
            key                = toleration.value
            operator           = "Exists"
            effect             = "NoExecute"
            toleration_seconds = local.fast_eviction_seconds
          }
        }
        container {
          name              = "replay"
          image             = "mdp-replay:${var.image_tag}"
          image_pull_policy = "IfNotPresent"
          port {
            name           = "http"
            container_port = 8000
          }
          env {
            name = "PG_DSN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db.metadata[0].name
                key  = "PG_DSN"
              }
            }
          }
          liveness_probe {
            http_get {
              path = "/healthz"
              port = 8000
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
          readiness_probe {
            http_get {
              path = "/readyz"
              port = 8000
            }
            initial_delay_seconds = 3
            period_seconds        = 3
            failure_threshold     = 2
          }
          resources {
            requests = { cpu = "50m", memory = "128Mi" }
            limits   = { cpu = "500m", memory = "384Mi" }
          }
        }
      }
    }
  }
}

# Headless services exist so Prometheus can resolve every pod IP by DNS and scrape all
# of them. A normal ClusterIP would load-balance the scrape and silently monitor one
# replica at random.
resource "kubernetes_service" "ingest_headless" {
  metadata {
    name      = "ingest-headless"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    cluster_ip                  = "None"
    publish_not_ready_addresses = true
    selector                    = { app = "ingest" }
    port {
      name = "http"
      port = 9100
    }
  }
}

resource "kubernetes_service" "replay_headless" {
  metadata {
    name      = "replay-headless"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    cluster_ip                  = "None"
    publish_not_ready_addresses = true
    selector                    = { app = "replay" }
    port {
      name = "http"
      port = 8000
    }
  }
}

resource "kubernetes_service" "replay" {
  metadata {
    name      = "replay"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    type     = "NodePort"
    selector = { app = "replay" }
    port {
      port        = 8000
      target_port = 8000
      node_port   = 30080
    }
  }
}
