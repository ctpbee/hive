import json
import os


class Config(dict):
    def __init__(self):
        super().__init__()

    def from_json(self, json_path: str):
        with open(json_path, "r") as f:
            obj = json.load(fp=f)
            self.update(obj)

    def from_mapping(self, mapping: dict):
        """
        """
        self.update(mapping)

    def from_env(self):
        for i, v in os.environ.items():
            if i.startswith("HIVE_"):
                key = i.split("_")[1]
                setattr(self, key, v)

    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value

    def as_dict(self):
        temp = self
        temp.update(self)
        return temp
