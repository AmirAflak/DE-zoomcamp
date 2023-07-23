FROM prefecthq/prefect

COPY docker-requirements.txt . 

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY gcp /opt/prefect/gcp
COPY deploys /opt/prefect/deploys
