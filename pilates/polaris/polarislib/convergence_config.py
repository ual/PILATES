from pathlib import Path

class ConvergenceConfig:
    def __init__(self, _data_dir='.', _db_name='campo'):
        self.data_dir= Path(_data_dir)
        self.db_name = _db_name