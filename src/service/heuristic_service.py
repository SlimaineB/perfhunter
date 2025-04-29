import importlib
import os


class HeuristicsService:
    def __init__(self):
        self.heuristics = []

    def get_heuristics(self):
        return self.heuristics

    
    def load_heuristics(self):
        heuristics = []
        heuristics_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "heuristics")
        for file in os.listdir(heuristics_dir):
            if file.endswith("_heuristic.py"):
                module_name = f"heuristics.{file[:-3]}"
                module = importlib.import_module(module_name)
                for attr in dir(module):
                    if attr.endswith("Heuristic"):
                        heuristic_class = getattr(module, attr)
                        if callable(heuristic_class) and hasattr(heuristic_class, "evaluate"):
                            heuristics.append(heuristic_class)
        self.heuristics = heuristics
        return self.heuristics 