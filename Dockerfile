FROM quay.io/astronomer/astro-runtime:12.4.0

# Crie o diretório para as credenciais
RUN mkdir -p /usr/local/airflow/gcloud

# Copia o arquivo google_cloud.json para o diretório
COPY /credentials/google_cloud.json /usr/local/airflow/gcloud/google_cloud.json

# Defina a variável de ambiente com o caminho correto para o arquivo
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/gcloud/google_cloud.json"
ENV KAGGLE_USERNAME=${KAGGLE_USERNAME}
ENV KAGGLE_KEY=${KAGGLE_KEY}