import duckdb
import pyarrow

class DuckDB:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self):
        self.cursor = duckdb.connect()

    def read_json(self, json_path: str, json_format:str = "array") -> pyarrow.lib.Table:
        return self.cursor.execute(f'SELECT * FROM read_json("{json_path}", format="{json_format}")').arrow()