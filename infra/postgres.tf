# Postgres is the one stateful piece. Its volume is provisioned by kind's local-path
# storage class, which means the data lives on one node's disk and cannot follow the
# pod elsewhere -- so the pod is pinned to the node labelled `data`, and the drills
# only ever kill the `compute` node. Being explicit about that beats discovering it
# during a drill.

resource "kubernetes_service" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    cluster_ip = "None"
    selector   = { app = "postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

resource "kubernetes_stateful_set" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    service_name = kubernetes_service.postgres.metadata[0].name
    replicas     = 1
    selector {
      match_labels = { app = "postgres" }
    }
    template {
      metadata {
        labels = { app = "postgres" }
      }
      spec {
        node_selector = { "mdp.io/role" = "data" }
        container {
          name  = "postgres"
          image = "postgres:17-alpine"
          port {
            container_port = 5432
          }
          env {
            name  = "POSTGRES_USER"
            value = "mdp"
          }
          env {
            name  = "POSTGRES_DB"
            value = "market"
          }
          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }
          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }
          readiness_probe {
            exec {
              command = ["pg_isready", "-U", "mdp", "-d", "market"]
            }
            initial_delay_seconds = 3
            period_seconds        = 3
            failure_threshold     = 3
          }
          liveness_probe {
            tcp_socket {
              port = 5432
            }
            initial_delay_seconds = 20
            period_seconds        = 10
          }
          resources {
            requests = { cpu = "100m", memory = "256Mi" }
            limits   = { cpu = "1000m", memory = "1Gi" }
          }
        }
      }
    }
    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "2Gi" }
        }
      }
    }
  }
}
