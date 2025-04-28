from flask import Flask
from services.spark_api import SparkAPI
from utils.recommendations import generate_data_skew_recommendations, evaluate_executor_memory
from config.settings import API_ENDPOINT

app = Flask(__name__)

@app.route('/analyze', methods=['GET'])
def analyze():
    spark_api = SparkAPI(API_ENDPOINT)
    job_data = spark_api.fetch_job_data()
    
    data_skew_recommendations = generate_data_skew_recommendations(job_data)
    executor_memory_evaluation = evaluate_executor_memory(job_data)
    
    return {
        "data_skew_recommendations": data_skew_recommendations,
        "executor_memory_evaluation": executor_memory_evaluation
    }

if __name__ == '__main__':
    app.run(debug=True)