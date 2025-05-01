mkdir spark-events && chmod -R 777 spark-events
mkdir minio-data && chmod -R 777 spark-events

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/calculate_pi.py


docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/bad_code.py

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/data_skew_s3.py

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/jobs/generate_skewed_data.py