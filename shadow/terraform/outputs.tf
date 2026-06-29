output "cluster_name" {
  description = "Shadow EKS cluster name."
  value       = module.eks.cluster_name
}

output "region" {
  value = var.region
}

output "kubectl_setup_command" {
  description = "Run this to load the shadow EKS context into kubeconfig under the alias `rp-shadow`."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name} --alias rp-shadow"
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "vpc_cidr" {
  value = var.vpc_cidr
}

output "peering_connection_id" {
  value = aws_vpc_peering_connection.shadow_to_stretch.id
}

output "rp_aws_node_sg_id" {
  description = "rp-aws node SG the shadow VPC was granted Kafka/Admin ingress on."
  value       = data.aws_security_group.rp_aws_node.id
}
