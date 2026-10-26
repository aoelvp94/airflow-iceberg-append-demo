FROM astrocrpublic.azurecr.io/runtime:3.3-7

COPY --chown=astro:0 iceberg_pkg /tmp/iceberg_pkg
RUN pip install --no-cache-dir /tmp/iceberg_pkg
