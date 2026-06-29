data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)

  # /20 public + /20 private subnets per AZ out of the /16. Nodes live in
  # private subnets behind a NAT (the shadow cluster doesn't need public
  # node IPs — unlike the stretch clusters it isn't in a Cilium ClusterMesh).
  public_subnets  = [for i, az in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  private_subnets = [for i, az in local.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  # rp-aws stretch cluster facts, read from its terraform state.
  rp_aws_vpc_id           = data.terraform_remote_state.aws_stretch.outputs.vpc_id
  rp_aws_vpc_cidr         = data.terraform_remote_state.aws_stretch.outputs.vpc_cidr
  rp_aws_private_rt_ids   = data.terraform_remote_state.aws_stretch.outputs.private_route_table_ids
  rp_aws_public_rt_ids    = data.terraform_remote_state.aws_stretch.outputs.public_route_table_ids
}
