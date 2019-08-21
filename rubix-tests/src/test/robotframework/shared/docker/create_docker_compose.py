#!/usr/bin/env python
import sys

master_str = "version: '3.7'\n" \
      "networks:\n" \
      "  default:\n" \
      "    external:\n" \
      "      name: network-rubix-build\n" \
      "services:\n" \
      "  rubix-master:\n" \
      "    build:\n" \
      "      context: .\n" \
      "      args:\n" \
      "        is_master: \"true\"\n" \
      "    volumes:\n" \
      "      - /tmp/rubix/tests:/tmp/rubix/tests\n" \
      "      - /tmp/rubix/jars:/usr/lib/rubix/lib\n" \
      "    networks:\n" \
      "      default:\n" \
      "        ipv4_address: 172.18.8.0\n"

worker_str= ":  \n" \
            "    build:\n" \
            "      context: .\n" \
            "      args:\n" \
            "        is-master: \"false\"\n" \
            "    volumes:\n" \
            "      - /tmp/rubix/tests:/tmp/rubix/tests\n" \
            "      - /tmp/rubix/jars:/usr/lib/rubix/lib\n" \
            "    networks:\n" \
            "      default:\n" \
            "        ipv4_address: "

no_of_workers =  sys.argv[1]
path = sys.argv[2]

docker_compose_yml = open(path+'/docker/docker-compose.yml',"w")
cluster_node_ips = open(path+'/docker/cluster_node_ips',"w")
print docker_compose_yml
print cluster_node_ips
docker_compose_yml.write(master_str)
for i in range(0, int(no_of_workers)):
    service_name = "  rubix-worker-" + str(i + 1)
    ipv_address = "172.18.8." + str(i + 1)
    cluster_node_ips.write(ipv_address + "\n")
    docker_compose_yml.write(service_name + worker_str + ipv_address+"\n")
docker_compose_yml.close()
cluster_node_ips.close()
