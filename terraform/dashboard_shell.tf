locals {
  pipeline_mode_label = var.enable_emr && var.enable_mwaa && var.enable_redshift ? "full" : "core-mode"
  dashboard_data_mode = local.pipeline_mode_label == "full" ? "auto" : "demo"
}

resource "aws_cloudwatch_log_group" "dashboard" {
  count             = var.enable_dashboard_shell ? 1 : 0
  name              = "/aws/ecs/streaming-etl-dashboard-${var.environment}"
  retention_in_days = 14
}

resource "aws_security_group" "dashboard_alb" {
  count       = var.enable_dashboard_shell ? 1 : 0
  name_prefix = "streaming-etl-dashboard-alb-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Public HTTP access"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "streaming-etl-dashboard-alb-sg"
  }
}

resource "aws_security_group" "dashboard_service" {
  count       = var.enable_dashboard_shell ? 1 : 0
  name_prefix = "streaming-etl-dashboard-svc-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "ALB to dashboard container"
    from_port       = 8501
    to_port         = 8501
    protocol        = "tcp"
    security_groups = [aws_security_group.dashboard_alb[0].id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "streaming-etl-dashboard-service-sg"
  }
}

resource "aws_lb" "dashboard" {
  count              = var.enable_dashboard_shell ? 1 : 0
  name               = "streaming-etl-dash-${var.environment}"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.dashboard_alb[0].id]
  subnets            = aws_subnet.public[*].id

  tags = {
    Name = "streaming-etl-dashboard-alb"
  }
}

resource "aws_lb_target_group" "dashboard" {
  count       = var.enable_dashboard_shell ? 1 : 0
  name        = "streaming-etl-dash-${var.environment}"
  port        = 8501
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    path                = "/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "dashboard_http" {
  count             = var.enable_dashboard_shell ? 1 : 0
  load_balancer_arn = aws_lb.dashboard[0].arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dashboard[0].arn
  }
}

resource "aws_ecs_cluster" "dashboard" {
  count = var.enable_dashboard_shell ? 1 : 0
  name  = "streaming-etl-dashboard-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_iam_role" "dashboard_task_execution" {
  count = var.enable_dashboard_shell ? 1 : 0
  name  = "streaming-etl-dashboard-task-exec-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dashboard_task_execution" {
  count      = var.enable_dashboard_shell ? 1 : 0
  role       = aws_iam_role.dashboard_task_execution[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_ecs_task_definition" "dashboard" {
  count                    = var.enable_dashboard_shell ? 1 : 0
  family                   = "streaming-etl-dashboard-${var.environment}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.dashboard_cpu)
  memory                   = tostring(var.dashboard_memory)
  execution_role_arn       = aws_iam_role.dashboard_task_execution[0].arn
  task_role_arn            = aws_iam_role.dashboard_task_execution[0].arn

  container_definitions = jsonencode([
    {
      name      = "dashboard"
      image     = "${aws_ecr_repository.dashboard.repository_url}:${var.dashboard_image_tag}"
      essential = true
      portMappings = [
        {
          containerPort = 8501
          hostPort      = 8501
          protocol      = "tcp"
        }
      ]
      environment = [
        {
          name  = "PIPELINE_MODE"
          value = local.pipeline_mode_label
        },
        {
          name  = "PIPELINE_DATA_MODE"
          value = local.dashboard_data_mode
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.dashboard[0].name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "dashboard" {
  count           = var.enable_dashboard_shell ? 1 : 0
  name            = "streaming-etl-dashboard-${var.environment}"
  cluster         = aws_ecs_cluster.dashboard[0].id
  task_definition = aws_ecs_task_definition.dashboard[0].arn
  desired_count   = var.dashboard_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.dashboard_service[0].id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.dashboard[0].arn
    container_name   = "dashboard"
    container_port   = 8501
  }

  depends_on = [aws_lb_listener.dashboard_http]
}
