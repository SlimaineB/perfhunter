def generate_data_skew_recommendations(job_data):
    recommendations = []
    # Analyze job_data for data skew
    for job in job_data:
        if job['dataSkew']:
            recommendations.append(f"Job {job['id']} has data skew issues. Consider repartitioning.")
    return recommendations

def evaluate_executor_memory(executor_data):
    recommendations = []
    # Analyze executor_data for memory usage
    for executor in executor_data:
        if executor['memoryUsed'] > executor['memoryAllocated'] * 0.8:
            recommendations.append(f"Executor {executor['id']} is using more than 80% of allocated memory. Consider increasing memory allocation.")
    return recommendations