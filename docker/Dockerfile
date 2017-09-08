#Siphon's base_image is derived from phusion/baseimage open source project

#FROM phusion/baseimage:0.9.21

FROM esnewdeveastdockerregistry.azurecr.io/esnewdeveastdockerregistry/siphon_baseimage:0.2

# Use baseimage-docker's init system.

CMD ["/sbin/my_init"]

# ...put your own build instructions here...

RUN mkdir -p /usr/bin/sphn/kat-service/kafka-availability-monitor
RUN mkdir -p /opt/e/kat/logs
COPY kafkaavailability-*.tar.gz /usr/bin/sphn/kat-service/kafka-availability-monitor/
RUN cd /usr/bin/sphn/kat-service/kafka-availability-monitor/ && tar xvzf kafkaavailability-*.tar.gz
RUN chmod a+x /usr/bin/sphn/kat-service/kafka-availability-monitor/runkat.sh

#copu base software
#COPY kafkaavailability-*.jar /usr/bin/sphn/kat-service/kafka-availability-monitor/
#COPY runkat.sh /usr/bin/sphn/kat-service/kafka-availability-monitor/
#RUN chmod a+x /usr/bin/sphn/kat-service/kafka-availability-monitor/runkat.sh

RUN mkdir /etc/service/kat && ln -s  /usr/bin/sphn/kat-service/kafka-availability-monitor/runkat.sh /etc/service/kat/run

#copy configuration, this is different for different clusters
#COPY kat_weveppe/*.properties kat_weveppe/*.json  /usr/bin/sphn/kat-service/kafka-availability-monitor/
#install netstat

RUN apt-get update
RUN apt-get install net-tools
RUN apt-get -y install telnet
##
## BEGIN Enable ssh
##
## END Enable ssh
# Clean up APT when done.

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*