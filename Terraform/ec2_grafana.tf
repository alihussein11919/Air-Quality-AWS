data "aws_iam_instance_profile" "lab_instance_profile" {
  name = "LabInstanceProfile"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "grafana" {
  name        = "${var.project_prefix}-grafana-sg"
  description = "Allow HTTP/HTTPS and SSH for Grafana"

  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 22
    to_port     = 22
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
    Project = var.project_prefix
    Layer   = "bi"
  }
}

resource "aws_instance" "grafana" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.micro"
  key_name               = var.key_pair_name
  vpc_security_group_ids = [aws_security_group.grafana.id]
  iam_instance_profile   = data.aws_iam_instance_profile.lab_instance_profile.name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update -y
              apt-get install -y apt-transport-https software-properties-common wget
              wget -q -O /usr/share/keyrings/grafana.key https://apt.grafana.com/gpg.key
              echo "deb [signed-by=/usr/share/keyrings/grafana.key] https://apt.grafana.com stable main" > /etc/apt/sources.list.d/grafana.list
              apt-get update -y
              apt-get install -y grafana
              grafana-cli plugins install grafana-athena-datasource
              systemctl enable grafana-server
              systemctl start grafana-server
              EOF

  tags = {
    Project = var.project_prefix
    Layer   = "bi"
  }
}
