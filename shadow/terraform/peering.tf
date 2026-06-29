# --- VPC peering: shadow VPC <-> rp-aws stretch VPC, both in us-east-1 ---
#
# This is what makes shadow-link replication traffic region-local. The
# shadow cluster fetches from the stretch cluster's AWS brokers over this
# peering, which rides the AWS backbone inside us-east-1 — it never touches
# the public internet and never crosses a cloud boundary. Combined with the
# stretch cluster's `default_leaders_preference: ordered_racks:aws,gcp,azure`
# (all partition leaders pinned to the AWS rack) and shadow linking's
# pull-from-leader fetch model, every replicated byte stays in us-east-1.

resource "aws_vpc_peering_connection" "shadow_to_stretch" {
  vpc_id      = module.vpc.vpc_id        # requester: shadow VPC
  peer_vpc_id = local.rp_aws_vpc_id      # accepter: rp-aws stretch VPC
  auto_accept = true                     # same account + region

  tags = {
    Name = "rp-shadow-to-rp-aws"
  }
}

# Routes on the SHADOW side: reach the rp-aws VPC CIDR via the peering.
# Static for_each keys (the route-table IDs themselves aren't known until
# apply, and for_each keys must be known at plan time). With
# single_nat_gateway = true the VPC module creates exactly one public and
# one private route table, so [0] is correct for each.
resource "aws_route" "shadow_to_stretch" {
  for_each = {
    public  = module.vpc.public_route_table_ids[0]
    private = module.vpc.private_route_table_ids[0]
  }
  route_table_id            = each.value
  destination_cidr_block    = local.rp_aws_vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.shadow_to_stretch.id
}

# Routes on the RP-AWS side: reach the shadow VPC CIDR via the peering.
# We write into the stretch cluster's route tables (ids read from its state)
# — same account, so this is allowed.
resource "aws_route" "stretch_to_shadow" {
  for_each = toset(concat(
    local.rp_aws_public_rt_ids,
    local.rp_aws_private_rt_ids,
  ))
  route_table_id            = each.value
  destination_cidr_block    = var.vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.shadow_to_stretch.id
}

# --- Open the rp-aws brokers to the shadow VPC ---
#
# Look up the rp-aws EKS node security group and allow ALL traffic inbound
# from the shadow VPC CIDR. We open everything (protocol -1) rather than a
# fixed port list because the exact advertised Kafka port the operator's
# external listener lands on can vary (9093 internal / 9094 external /
# NodePort range), and the broker also advertises an Admin/RPC path the
# shadow side may touch. This mirrors the stretch stack's own
# `peer_cloud_all` rule (aws/terraform/eks.tf), which opens all traffic from
# the peer-cloud CIDRs. Tighten to specific ports in production.
data "aws_security_group" "rp_aws_node" {
  vpc_id = local.rp_aws_vpc_id
  filter {
    name   = "tag:Name"
    values = [var.rp_aws_node_sg_name_tag]
  }
}

resource "aws_security_group_rule" "rp_aws_from_shadow" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [var.vpc_cidr]
  security_group_id = data.aws_security_group.rp_aws_node.id
  description       = "all traffic from rp-shadow VPC (shadow-link, VPC-peered)"
}
