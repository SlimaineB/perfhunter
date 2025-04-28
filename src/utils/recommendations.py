def generate_data_skew_recommendations(job_data):
    recommendations = []
    # Analyze job_data for data skew
    for job in job_data:
        # Exemple simple : si numCompletedTasks est très différent de numTasks, possible skew
        if job['numTasks'] > 0 and job['numCompletedTasks'] < job['numTasks']:
            recommendations.append(
                f"Job {job['jobId']} may have data skew issues: only {job['numCompletedTasks']} out of {job['numTasks']} tasks completed. Consider repartitioning."
            )
    return recommendations

def evaluate_executor_memory(executor_data):
    recommendations = []
    # Analyze executor_data for memory usage
    for executor in executor_data:
        if executor['maxMemory'] > 0 and executor['memoryUsed'] > executor['maxMemory'] * 0.8:
            recommendations.append(
                f"Executor {executor['id']} is using more than 80% of allocated memory. Consider increasing memory allocation."
            )
    return recommendations