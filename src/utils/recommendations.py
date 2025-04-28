def generate_data_skew_recommendations(job_data):
    recommendations = []
    for job in job_data:
        if job['numTasks'] > 0:
            completion_ratio = job['numCompletedTasks'] / job['numTasks']
            if completion_ratio < 0.8:
                recommendations.append(
                    f"Job {job['jobId']} ({job['name']}) : Seulement {job['numCompletedTasks']} sur {job['numTasks']} tâches terminées. Vérifiez un éventuel data skew ou des partitions déséquilibrées."
                )
            if job['numFailedTasks'] > 0:
                recommendations.append(
                    f"Job {job['jobId']} ({job['name']}) : {job['numFailedTasks']} tâches ont échoué. Analysez les logs pour identifier la cause."
                )
            if job['numSkippedTasks'] > 0:
                recommendations.append(
                    f"Job {job['jobId']} ({job['name']}) : {job['numSkippedTasks']} tâches ont été ignorées. Cela peut indiquer des problèmes de données ou de configuration."
                )
    if not recommendations:
        recommendations.append("Aucun problème de skew ou d'échec de tâches détecté sur les jobs analysés.")
    return recommendations

def evaluate_executor_memory(executor_data):
    recommendations = []
    for executor in executor_data:
        # Ignore driver for recommendations
        if executor['id'] == 'driver':
            continue

        # Mémoire utilisée
        if executor['maxMemory'] > 0:
            usage = executor['memoryUsed'] / executor['maxMemory']
            if usage > 0.8:
                recommendations.append(
                    f"Exécuteur {executor['id']} utilise {usage:.0%} de la mémoire allouée. Envisagez d'augmenter la mémoire ou d'optimiser les tâches."
                )

        # Blacklist
        if executor.get('isBlacklisted', False):
            recommendations.append(
                f"Exécuteur {executor['id']} est blacklisté. Vérifiez les erreurs et envisagez de le redémarrer."
            )

        # Temps passé en GC
        if executor['totalGCTime'] > 0 and executor['totalDuration'] > 0:
            gc_ratio = executor['totalGCTime'] / executor['totalDuration']
            if gc_ratio > 0.2:
                recommendations.append(
                    f"Exécuteur {executor['id']} passe plus de 20% du temps en GC. Essayez d'augmenter la mémoire ou d'optimiser les objets créés."
                )

        # Vérifier si aucun core n'est alloué
        if executor['totalCores'] == 0:
            recommendations.append(
                f"Exécuteur {executor['id']} n'a aucun core alloué. Vérifiez la configuration du cluster."
            )

        # Vérifier si aucun shuffle n'est effectué
        if executor['totalShuffleRead'] == 0 and executor['totalShuffleWrite'] == 0 and executor['completedTasks'] > 0:
            recommendations.append(
                f"Exécuteur {executor['id']} n'a effectué aucune opération de shuffle. Cela peut être normal, mais vérifiez si attendu."
            )

        # Vérifier si aucune tâche n'a échoué mais beaucoup de tâches terminées
        if executor['failedTasks'] == 0 and executor['completedTasks'] > 0:
            recommendations.append(
                f"Exécuteur {executor['id']} a terminé toutes ses tâches sans échec. Performance optimale."
            )

        # Vérifier si l'exécuteur est inactif mais a des tâches à faire
        if not executor['isActive'] and executor['activeTasks'] > 0:
            recommendations.append(
                f"Exécuteur {executor['id']} est inactif alors qu'il a des tâches actives. Vérifiez l'état du cluster."
            )

    if not recommendations:
        recommendations.append("Aucun problème de mémoire ou d'exécution détecté sur les exécutors analysés.")
    return recommendations

def generate_stage_recommendations(stage_data):
    recommendations = []
    for stage in stage_data:
        # Vérifier si le stage a échoué
        if stage.get('numFailedTasks', 0) > 0:
            recommendations.append(
                f"Stage {stage['stageId']} ({stage['name']}) : {stage['numFailedTasks']} tâches ont échoué. Analysez les logs pour comprendre la cause."
            )
        # Vérifier les spills mémoire/disque
        if stage.get('memoryBytesSpilled', 0) > 0:
            recommendations.append(
                f"Stage {stage['stageId']} ({stage['name']}) : {stage['memoryBytesSpilled']} octets ont été spillés en mémoire. Essayez d'augmenter la mémoire ou d'optimiser les partitions."
            )
        if stage.get('diskBytesSpilled', 0) > 0:
            recommendations.append(
                f"Stage {stage['stageId']} ({stage['name']}) : {stage['diskBytesSpilled']} octets ont été spillés sur disque. Cela peut indiquer un manque de mémoire."
            )
        # Vérifier l'absence de shuffle
        if stage.get('shuffleReadBytes', 0) == 0 and stage.get('shuffleWriteBytes', 0) == 0 and stage.get('numTasks', 0) > 0:
            recommendations.append(
                f"Stage {stage['stageId']} ({stage['name']}) : aucune opération de shuffle détectée. Cela peut être normal, mais vérifiez si attendu."
            )
        # Vérifier si le stage a pris beaucoup de temps
        if stage.get('completionTime') and stage.get('submissionTime'):
            from datetime import datetime
            fmt = "%Y-%m-%dT%H:%M:%S.%fGMT"
            try:
                start = datetime.strptime(stage['submissionTime'], fmt)
                end = datetime.strptime(stage['completionTime'], fmt)
                duration = (end - start).total_seconds()
                if duration > 60:
                    recommendations.append(
                        f"Stage {stage['stageId']} ({stage['name']}) : durée élevée ({duration:.1f}s). Optimisez les transformations ou vérifiez les ressources."
                    )
            except Exception:
                pass
        # Vérifier si aucun enregistrement n'a été lu ou écrit
        if stage.get('inputRecords', 0) == 0 and stage.get('outputRecords', 0) == 0:
            recommendations.append(
                f"Stage {stage['stageId']} ({stage['name']}) : aucun enregistrement lu ou écrit. Vérifiez la logique de votre pipeline."
            )
    if not recommendations:
        recommendations.append("Aucun problème détecté sur les stages analysés.")
    return recommendations