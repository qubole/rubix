#!/usr/bin/env python
import sys

master_str = """version: '3.7'
networks:
  default:
    external:
      name: network-rubix-build
services:
  rubix-master:
    build:
      context: .
      args:
        is_master: \"true\"
    volumes:
      - /tmp/rubix/tests:/tmp/rubix/tests
      - /tmp/rubix/jars:/usr/lib/rubix/lib
    networks:
      default:
        ipv4_address: 172.18.8.0\n"""


worker_str = """{}:
    build:
      context: .
      args:
        is-master: \"false\"
    volumes:
      - /tmp/rubix/tests:/tmp/rubix/tests
      - /tmp/rubix/jars:/usr/lib/rubix/lib
    networks:
      default:
        ipv4_address: {}"""

no_of_workers =  sys.argv[1]
path = sys.argv[2]
cluster_node_ips_path = sys.argv[3]
with open(path+'/docker/docker-compose.yml',"w") as docker_compose_yml, open(cluster_node_ips_path,"w") as cluster_node_ips:
    docker_compose_yml.write(master_str)
    for i in range(0, int(no_of_workers)):
        service_name = "  rubix-worker-{}".format(str(i + 1))
        ipv_address = "172.18.8.{}".format(str(i + 1))
        cluster_node_ips.write(ipv_address + "\n")
        docker_compose_yml.write(worker_str.format(service_name, ipv_address+"\n"))
