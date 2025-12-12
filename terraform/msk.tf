resource "aws_msk_cluster" "main" {
  cluster_name           = "streaming-etl-kafka-${var.environment}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = var.msk_broker_count

  broker_node_group_info {
    instance_type  = var.msk_instance_type
    client_subnets = aws_subnet.private[*].id
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
    security_groups = [aws_security_group.default.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = {
    Name = "streaming-etl-kafka"
  }
}

resource "aws_msk_configuration" "main" {
  name              = "streaming-etl-kafka-config-${var.environment}"
  kafka_versions    = ["3.5.1"]
  server_properties = <<PROPERTIES
auto.create.topics.enable=true
default.replication.factor=3
min.insync.replicas=2
num.io.threads=8
num.network.threads=5
num.partitions=6
num.replica.fetchers=2
socket.request.max.bytes=104857600
unclean.leader.election.enable=false
log.retention.hours=168
PROPERTIES
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/streaming-etl-${var.environment}"
  retention_in_days = 14
}
