docker compose down

mkdir spark-events && chmod -R 777 spark-events
mkdir minio-data && chmod -R 777 minio-data

docker compose up -d 
