############################################################
# Dockerfile to build MySQL test container with proper schema
# Based on MySQL image
############################################################

FROM mysql

MAINTAINER Mircea Alexandru <mircea.alexandru@gmail.com>

ENV DEBIAN_FRONTEND noninteractive

############################################################
# Create test schema
############################################################

ADD schema.sql /tmp/schema.sql
ADD init_db.sh /tmp/init_db.sh

RUN chmod a+x /tmp/init_db.sh
RUN /tmp/init_db.sh

EXPOSE 3306
CMD ["mysqld"]
