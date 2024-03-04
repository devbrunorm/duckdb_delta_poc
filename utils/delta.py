from deltalake import DeltaTable, write_deltalake
import pyarrow

class CustomDeltaTable:
    _path: str
    _dt: DeltaTable

    def __init__(self, path: str, data: pyarrow.lib.Table = None):
        if data:
            try:
                self._dt = self.create(path, data)
            except:
                raise ValueError("Delta Table não existente. Informe os dados para criação da mesma.")
        else:
            self._dt = self._load(path)

    def show(self):
        print(self._dt.to_pandas())

    def _load(self, path: str) -> DeltaTable:
        self._path = path
        self._dt = DeltaTable(path)
        return self._dt

    def create(self, path:str, data: pyarrow.lib.Table) -> DeltaTable:
        write_deltalake(path, data)
        return self._load(path)

    def upsert(self, source: pyarrow.lib.Table, keys: dict) -> DeltaTable:
        predicate_components = []
        for target_col, source_col in keys.items():
            predicate_components.append(f"target.{target_col} = source.{source_col}")
        predicate = " AND ".join(predicate_components)
        self._dt\
            .merge(
                source = source, 
                predicate = predicate, 
                source_alias="target", 
                target_alias="source")\
            .when_matched_update_all()\
            .when_not_matched_insert_all()\
            .execute()
        self._dt = self._load(self._path)
        return self

    def delete(self, source: pyarrow.lib.Table, keys: dict) -> DeltaTable:
        predicate_components = []
        for target_col, source_col in keys.items():
            predicate_components.append(f"target.{target_col} = source.{source_col}")
        predicate = " AND ".join(predicate_components)
        self._dt\
            .merge(
                source = source, 
                predicate = predicate, 
                source_alias="target", 
                target_alias="source")\
            .when_matched_delete()\
            .execute()
        self._dt = self._load(self._path)
        return self