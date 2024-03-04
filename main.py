
from utils.delta import CustomDeltaTable
from utils.duckdb import DuckDB
import os

DELTA_TABLE_PATH = "./data/delta"
INSERT_JSON_PATH = "./examples/first_load.json"
DELETE_JSON_PATH = "./examples/remove_sample.json"
UPSERT_JSON_PATH = "./examples/upsert_sample.json"

class MissingInsertJsonException(Exception):
    pass

def delete_json(delta_table:CustomDeltaTable, json_path:str):
    deletes = DuckDB().read_json(json_path, json_format = "array")
    return delta_table.delete(
        source = deletes,
        keys = {
            "id": "id"
        }
    )

def upsert_json(delta_table:CustomDeltaTable, json_path:str):
    updates = DuckDB().read_json(UPSERT_JSON_PATH, json_format = "array")
    return delta_table.upsert(
        source = updates,
        keys = {
            "id": "id"
        }
    )

if os.path.isdir(DELTA_TABLE_PATH):
    delta_table = CustomDeltaTable(DELTA_TABLE_PATH)
    if os.path.exists(DELETE_JSON_PATH):
        delta_table = delete_json(delta_table, DELETE_JSON_PATH)
    if os.path.exists(UPSERT_JSON_PATH):
        delta_table = upsert_json(delta_table, UPSERT_JSON_PATH)
else:
    if os.path.exists(UPSERT_JSON_PATH):
        inserts = DuckDB().read_json(INSERT_JSON_PATH, json_format = "array")
        delta_table = CustomDeltaTable(DELTA_TABLE_PATH, inserts)
    else:
        raise Exception("Delta Table não existente. Necessário JSON de inserção de dados para criá-la.")

delta_table.show()