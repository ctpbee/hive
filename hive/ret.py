from typing import List
import numpy as np


class TakeTemplate:

    def as_array(self) -> np.array:
        raise NotImplemented

    def as_dataframe(self) -> np.array:
        raise NotImplemented

    def as_list(self) -> List:
        raise NotImplemented

    def take(self):
        raise NotImplemented
