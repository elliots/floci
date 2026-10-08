resource "aws_elasticache_user" "default" {
  user_id              = "tf-group-default"
  user_name            = "default"
  engine               = "REDIS"
  access_string        = "on ~* +@all"
  no_password_required = true
  tags                 = { Environment = "compatibility" }
}

resource "aws_elasticache_user" "extra" {
  user_id              = "tf-group-extra"
  user_name            = "extra"
  engine               = "REDIS"
  access_string        = "on ~* +@all"
  no_password_required = true
  tags                 = { Environment = "compatibility" }
}

resource "aws_elasticache_user_group" "test" {
  user_group_id = "tf-user-group"
  engine        = "REDIS"
  user_ids      = [aws_elasticache_user.default.user_id]
  tags          = { Environment = "compatibility" }

  lifecycle {
    ignore_changes = [user_ids]
  }
}

resource "aws_elasticache_user_group_association" "extra" {
  user_group_id = aws_elasticache_user_group.test.user_group_id
  user_id       = aws_elasticache_user.extra.user_id
}

resource "aws_default_subnet" "test" {
  availability_zone = "us-east-1a"
  force_destroy     = true
  tags              = { Environment = "compatibility" }
}
