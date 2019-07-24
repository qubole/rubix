FROM jordanwbitquill/rubix-integration-test-base:0.3.5-SNAPSHOT

####################
### CUSTOM SETUP ###
####################

CMD ["/sbin/my_init"]

# Setup logging
RUN mkdir /home/logs/
COPY logging/log4j_bks.properties \
    logging/log4j_lds.properties \
    logging/log4j_crs.properties \
    /home/props/

####################
### DAEMON SETUP ###
####################

ARG is_master
ENV IS_CLUSTER_MASTER=$is_master

### CONTAINER REQUESTS ###

RUN mkdir /etc/service/crs
COPY start-crs.sh /etc/service/crs/run
RUN chmod +x /etc/service/crs/run

### BOOKKEEPER ###

RUN mkdir /etc/service/bks
COPY start-bks.sh /etc/service/bks/run
RUN chmod +x /etc/service/bks/run

### LOCAL DATA TRANSFER ###

RUN mkdir /etc/service/lds
COPY start-lds.sh /etc/service/lds/run
RUN chmod +x /etc/service/lds/run
