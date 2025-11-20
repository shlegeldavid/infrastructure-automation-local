variable "timeweb_token" {
    description = "Timeweb API Token"
    type        = string
    sensitive   = true
}

variable "ssh_public_key" {
  description = "Content of the public SSH key (string)"
  type        = string
}