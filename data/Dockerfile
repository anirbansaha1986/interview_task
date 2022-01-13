FROM mysql:5.6

ENV MYSQL_DATABASE=mysql \
    MYSQL_ROOT_PASSWORD=password

ADD data_dumps/* /docker-entrypoint-initdb.d

EXPOSE 3306