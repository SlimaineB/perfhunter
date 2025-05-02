import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ShuffleWriteHeuristic(BaseHeuristic):
    """Heuristic for evaluating shuffle write performance in Spark stages."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        stage_data = all_data.get("stages", [])
        shuffle_write_threshold = THRESHOLDS.get("shuffle_write_bytes", 1e9)  # 1 GB

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            shuffle_write = stage.get("shuffleWriteBytes", 0)

            criticity = Criticity.HIGH if shuffle_write > shuffle_write_threshold else Criticity.NONE
            ShuffleWriteHeuristic.add_check(
                checks, "Shuffle Write", f"<= {shuffle_write_threshold / 1e6:.2f} MB", 
                f"{shuffle_write / 1e6:.2f} MB", f"Stage {stage_id} writes a large amount of shuffle data.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = ShuffleWriteHeuristic.evaluate(all_data)
# print(df)
