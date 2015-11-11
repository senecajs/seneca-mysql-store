############################################################
# Dockerfile to build MySQL test container with proper schema
# Based on MySQL image
############################################################

FROM malex/mysql

MAINTAINER Mircea Alexandru <mircea.alexandru@gmail.com>

ENV DEBIAN_FRONTEND noninteractive

############################################################
# Create test schema
############################################################

COPY schema.sql /tmp/schema.sql
COPY init_db.sh /tmp/init_db.sh

RUN chmod a+x /tmp/init_db.sh

RUN /tmp/init_db.sh

ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 3306
CMD ["mysqld"]
