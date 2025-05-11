docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.app.name="MemoryAnalysisJob 1 core/1 instance/1g " \
  /opt/spark-apps/jobs/memory_job.py


docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=20 \
  --conf spark.cores.max=20 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.app.name="MemoryAnalysisJob 4 core/3 instance/1g " \
  /opt/spark-apps/jobs/memory_job.py


docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=20 \
  --conf spark.cores.max=60 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.app.name="MemoryAnalysisJob 20 cores/3 instance/2g " \
  /opt/spark-apps/jobs/memory_job.py
