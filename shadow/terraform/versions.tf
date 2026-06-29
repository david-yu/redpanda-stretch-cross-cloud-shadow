terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    # Same reason as aws/terraform: install a CSI-backed default
    # StorageClass (`ebs-sc`) so the shadow cluster's broker PVCs bind on
    # EKS 1.34 (the in-tree `kubernetes.io/aws-ebs` provisioner is gone).
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}
