variable "project_name" {
  description = "Tag value for cost allocation / cleanup. Matches the stretch stacks so a tag-based sweep catches the shadow cluster too."
  type        = string
  default     = "redpanda-stretch-cross-cloud"
}

variable "owner" {
  description = "Tag value identifying the owner."
  type        = string
  default     = "redpanda-operator-stretch-cross-cloud-beta"
}

variable "region" {
  description = "AWS region for the shadow EKS cluster. MUST be the same region as the stretch cluster's AWS cluster (us-east-1) so shadow-link replication traffic is region-local — the stretch cluster pins all partition leaders to the AWS rack, and shadow linking is a pull-based fetch from leaders, so keeping the shadow cluster in us-east-1 means every replicated byte stays inside us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the shadow EKS cluster (also the kubectl context alias after rename)."
  type        = string
  default     = "rp-shadow"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.34"
}

# Shadow VPC CIDR. MUST NOT overlap the rp-aws VPC (10.10.0.0/16) since the
# two VPCs are peered, and ideally not the other clouds' CIDRs either
# (10.20/16, 10.30/16) to keep the address plan clean. 10.40.0.0/16 is free.
variable "vpc_cidr" {
  description = "Shadow VPC CIDR. Must NOT overlap the rp-aws VPC CIDR (10.10.0.0/16) — they're peered."
  type        = string
  default     = "10.40.0.0/16"
}

variable "node_instance_type" {
  description = "EC2 instance type for the shadow node group."
  type        = string
  default     = "m5.xlarge"
}

variable "node_count" {
  description = "Desired/min node count. 3 nodes hosts a 3-broker RF=3 shadow cluster across 3 AZs in us-east-1."
  type        = number
  default     = 3
}

variable "node_volume_size_gb" {
  description = "Root EBS volume size for each node (GiB)."
  type        = number
  default     = 50
}

# --- Cross-stack wiring to the stretch cluster's AWS cluster (rp-aws) ---

variable "aws_stretch_state_path" {
  description = "Path to the rp-aws stretch cluster's local terraform state, read to discover its VPC id and route tables for peering."
  type        = string
  default     = "../../aws/terraform/terraform.tfstate"
}

variable "rp_aws_node_sg_name_tag" {
  description = "Value of the `Name` tag on the rp-aws EKS node security group (terraform-aws-modules/eks v20 names it `<cluster_name>-node`). Used to look up the SG so we can open it to the shadow VPC."
  type        = string
  default     = "rp-aws-node"
}
