FROM postgres:18-alpine

RUN apk add --no-cache python3 py3-pip su-exec \
    && apk add --no-cache --virtual .build-deps gcc musl-dev python3-dev postgresql-dev \
    && pip3 install --no-cache-dir --break-system-packages \
         'patroni[kubernetes,jsonlogger]==3.3.6' \
         'psycopg2-binary>=2.5.4' \
    && apk del .build-deps \
    && rm -rf /root/.cache

ENV PATRONICTL_CONFIG_FILE=/etc/patroni/patroni.yml
ENV PGDATA=/var/lib/postgresql/data/pgdata

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5432 8000
ENTRYPOINT ["/entrypoint.sh"]
