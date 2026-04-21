FROM spark:3.5.8-python3

USER 0

RUN python3 -m pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary

WORKDIR /opt/spark/work-dir

CMD ["sleep", "infinity"]
