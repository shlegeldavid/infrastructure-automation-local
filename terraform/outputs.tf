output "server_ip" {
    value = twc_floating_ip.main_ip.ip
    description = "Public IP of created server"
}

output "server_id" {
    value = twc_server.app_server.id
}
