terraform {
    required_providers {
        twc = {
            source = "tf.timeweb.cloud/timeweb-cloud/timeweb-cloud"
        }
    }
    required_version = ">= 0.13"
}

provider "twc" {
    token = var.timeweb_token
}

resource "twc_ssh_key" "main" {
    name = "devops-key"
    body = var.ssh_public_key
}

resource "twc_floating_ip" "main_ip" {
	availability_zone = "spb-3"
	ddos_guard = false
}

resource "twc_server" "app_server" {
	name = "My-App-Server"
	preset_id = 2453
	project_id = 1936944
	os_id = 99
	availability_zone = "spb-3"
	is_root_password_required = true
	ssh_keys_ids = [twc_ssh_key.main.id]
	floating_ip_id = twc_floating_ip.main_ip.id

	local_network {
	}
}