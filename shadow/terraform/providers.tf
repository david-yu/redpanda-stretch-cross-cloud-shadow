provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project = var.project_name
      Owner   = var.owner
      Role    = "shadow"
    }
  }
}

# Authenticate the kubernetes provider against the shadow EKS cluster via
# `aws eks get-token` exec auth (same lazy-binding pattern as aws/terraform
# — provider config is per-graph, so exec auth defers endpoint/CA resolution
# until apply time after the EKS module has created the cluster).
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks", "get-token",
      "--cluster-name", module.eks.cluster_name,
      "--region", var.region,
    ]
  }
}

# Read the rp-aws stretch cluster's state so we can peer the shadow VPC to
# it and add the cross-VPC routes on the rp-aws side. Local backend because
# the stretch stacks use local state (see aws/terraform/).
data "terraform_remote_state" "aws_stretch" {
  backend = "local"
  config = {
    path = var.aws_stretch_state_path
  }
}
