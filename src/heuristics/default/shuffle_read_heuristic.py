import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ShuffleReadHeuristic(BaseHeuristic):
    """Heuristic for evaluating shuffle read performance in Spark stages."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        stage_data = all_data.get("stages", [])
        shuffle_read_threshold = THRESHOLDS["shuffle_read_bytes"]
        shuffle_wait_time_threshold = THRESHOLDS.get("shuffle_wait_time_ms", 5000)  # Max wait time in ms
        remote_vs_local_ratio_threshold = THRESHOLDS.get("remote_vs_local_ratio", 2)  # Max remote/local ratio

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")

            # 1. Check shuffle read size
            shuffle_read = stage.get("shuffleReadBytes", 0)
            criticity = Criticity.HIGH if shuffle_read > shuffle_read_threshold else Criticity.NONE
            ShuffleReadHeuristic.add_check(
                checks, "Shuffle Read", f"<= {shuffle_read_threshold / 1e6:.2f} MB", 
                f"{shuffle_read / 1e6:.2f} MB", f"Stage {stage_id} reads a large amount of shuffle data.", criticity
            )

            # 2. Check shuffle wait time
            shuffle_wait_time = stage.get("shuffleFetchWaitTime", 0)
            criticity = Criticity.MEDIUM if shuffle_wait_time > shuffle_wait_time_threshold else Criticity.NONE
            ShuffleReadHeuristic.add_check(
                checks, "Shuffle Performance", f"<= {shuffle_wait_time_threshold} ms",
                f"{shuffle_wait_time} ms", f"Stage {stage_id} experiences high shuffle read wait time.", criticity
            )

            # 3. Check remote vs local shuffle read ratio
            remote_bytes = stage.get("shuffleRemoteBytesRead", 0)
            local_bytes = stage.get("shuffleLocalBytesRead", 0)  # Prevent division by zero
            if local_bytes is not None and remote_bytes is not None and local_bytes >0 :
                remote_vs_local_ratio = remote_bytes / local_bytes
                criticity = Criticity.SEVERE if (remote_vs_local_ratio is not None and remote_vs_local_ratio > remote_vs_local_ratio_threshold) else Criticity.NONE
                ShuffleReadHeuristic.add_check(
                    checks, "Data Locality", f"<= {remote_vs_local_ratio_threshold}",
                    f"{remote_vs_local_ratio:.2f}", f"Stage {stage_id} has high remote shuffle read ratio.", criticity
                )

            # 4. Check remote blocks vs local blocks fetched
            remote_blocks = stage.get("shuffleRemoteBlocksFetched", 0)
            local_blocks = stage.get("shuffleLocalBlocksFetched", 0)
            criticity = Criticity.MODERATE if remote_blocks > local_blocks else Criticity.NONE
            ShuffleReadHeuristic.add_check(
                checks, "Shuffle Efficiency", "Remote blocks <= Local blocks",
                f"{remote_blocks} vs {local_blocks}", 
                f"Stage {stage_id} fetches more remote blocks than local blocks, potentially increasing network latency.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = ShuffleReadHeuristic.evaluate(all_data)
# print(df)
