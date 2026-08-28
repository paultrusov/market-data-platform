# Prometheus scrapes both tiers by DNS over their headless services, evaluates the
# alert rules, and is reachable on the host at :30090.
#
# No Alertmanager: the alerts are the artifact, and routing them to a chat channel
# adds a component without adding anything to demonstrate. `ALERTS{alertstate="firing"}`
# is queryable directly, which is what the drills assert against.

resource "kubernetes_config_map" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  data = {
    "prometheus.yml" = yamlencode({
      global = {
        scrape_interval     = "5s"
        evaluation_interval = "5s"
      }
      rule_files = ["/etc/prometheus/alerts.yml"]
      scrape_configs = [
        {
          job_name       = "ingest"
          dns_sd_configs = [{ names = ["ingest-headless.${var.namespace}.svc.cluster.local"], type = "A", port = 9100 }]
        },
        {
          job_name       = "replay"
          dns_sd_configs = [{ names = ["replay-headless.${var.namespace}.svc.cluster.local"], type = "A", port = 8000 }]
        },
      ]
    })

    "alerts.yml" = yamlencode({
      groups = [{
        name = "market-data"
        rules = [
          {
            # The one that matters: data has stopped arriving. Every other symptom
            # (a crashed pod, a dead node, a broken feed) shows up here too.
            alert = "IngestStalled"
            # `or absent(...)` is not decoration. When the last ingest pod goes away
            # its series goes with it, rate() has nothing to evaluate, and a bare
            # `== 0` matches nothing -- the alert falls silent at the exact moment the
            # thing it watches is most dead.
            expr        = "sum(rate(mdp_ticks_ingested_total[1m])) == 0 or absent(mdp_ticks_ingested_total)"
            for         = "30s"
            labels      = { severity = "critical" }
            annotations = { summary = "No trades written in the last minute" }
          },
          {
            # Data is arriving, but late -- a slow consumer or a backed-up database,
            # which a liveness probe will never catch because the process is fine.
            alert       = "IngestLagHigh"
            expr        = "max(mdp_ingest_lag_seconds) > 10"
            for         = "1m"
            labels      = { severity = "warning" }
            annotations = { summary = "Ingest is more than 10s behind the exchange clock" }
          },
          {
            alert       = "IngestReplicaDown"
            expr        = "count(up{job=\"ingest\"} == 1) < ${var.ingest_replicas} or absent(up{job=\"ingest\"})"
            for         = "1m"
            labels      = { severity = "warning" }
            annotations = { summary = "Fewer ingest replicas up than configured" }
          },
          {
            alert       = "ReplayErrors"
            expr        = "sum(rate(mdp_replay_errors_total[5m])) > 0"
            for         = "1m"
            labels      = { severity = "warning" }
            annotations = { summary = "Replay API is returning errors" }
          },
        ]
      }]
    })
  }
}

resource "kubernetes_deployment" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "prometheus" }
    }
    template {
      metadata {
        labels      = { app = "prometheus" }
        annotations = { "checksum/config" = sha1(jsonencode(kubernetes_config_map.prometheus.data)) }
      }
      spec {
        container {
          name  = "prometheus"
          image = "prom/prometheus:v3.1.0"
          args = [
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.retention.time=2h",
            "--web.enable-lifecycle",
          ]
          port {
            container_port = 9090
          }
          volume_mount {
            name       = "config"
            mount_path = "/etc/prometheus"
          }
          volume_mount {
            name       = "data"
            mount_path = "/prometheus"
          }
          readiness_probe {
            http_get {
              path = "/-/ready"
              port = 9090
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
          resources {
            requests = { cpu = "50m", memory = "192Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }
        }
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.prometheus.metadata[0].name
          }
        }
        volume {
          name = "data"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "prometheus" {
  metadata {
    name      = "prometheus"
    namespace = kubernetes_namespace.mdp.metadata[0].name
  }
  spec {
    type     = "NodePort"
    selector = { app = "prometheus" }
    port {
      port        = 9090
      target_port = 9090
      node_port   = 30090
    }
  }
}
