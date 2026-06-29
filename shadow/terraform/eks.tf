module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  vpc_id = module.vpc.vpc_id
  # Nodes in PRIVATE subnets. The shadow cluster is a standalone Redpanda
  # cluster — it is NOT part of the Cilium ClusterMesh, so its nodes don't
  # need public IPs or cross-cloud reachability. They only need to reach the
  # stretch cluster's rp-aws brokers, which happens over the VPC peering
  # (see peering.tf) on private addresses inside us-east-1.
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true

  # Standard EKS networking for a standalone cluster — keep the AWS VPC CNI
  # (vpc-cni), kube-proxy, and coredns. Unlike the stretch clusters we do
  # NOT swap in Cilium: there's no clustermesh to join, and VPC-CNI pod IPs
  # (from the 10.40/16 VPC) keep things simple. aws-ebs-csi-driver is
  # required on 1.34 for broker PVC binding (same gotcha as the stretch
  # cluster — the in-tree aws-ebs provisioner was removed).
  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
    aws-ebs-csi-driver = {
      most_recent                 = true
      resolve_conflicts_on_update = "OVERWRITE"
    }
  }

  eks_managed_node_groups = {
    default = {
      desired_size = var.node_count
      min_size     = var.node_count
      max_size     = var.node_count + 1

      instance_types = [var.node_instance_type]
      subnet_ids     = module.vpc.private_subnets

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.node_volume_size_gb
            volume_type           = "gp3"
            delete_on_termination = true
            encrypted             = true
          }
        }
      }

      iam_role_additional_policies = {
        AmazonEBSCSIDriverPolicy = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      }
    }
  }

  enable_cluster_creator_admin_permissions = true
}

# CSI-backed default StorageClass — required on EKS 1.34 so the shadow
# brokers' PVCs bind (same rationale as aws/terraform/eks.tf).
resource "kubernetes_storage_class_v1" "ebs_sc" {
  metadata {
    name = "ebs-sc"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }
  storage_provisioner    = "ebs.csi.aws.com"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  parameters = {
    type      = "gp3"
    encrypted = "true"
  }

  depends_on = [module.eks]
}
