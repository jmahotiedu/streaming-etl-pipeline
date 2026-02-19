resource "aws_ecr_repository" "producer" {
  name                 = "streaming-etl-producer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "spark" {
  name                 = "streaming-etl-spark"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep latest 20 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

resource "aws_ecr_lifecycle_policy" "spark" {
  repository = aws_ecr_repository.spark.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep latest 20 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}
