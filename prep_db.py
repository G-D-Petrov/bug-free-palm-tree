from arcticdb import Arctic
import pandas as pd

ac = Arctic("lmdb://data/arcticdb")
ac.delete_library("vel_test")
lib = ac.get_library("vel_test", create_if_missing=True)


for sym in range(5):
    sym_name = f"sym_{sym}"
    for i in range(100):
        df = pd.DataFrame({"data": [i]}, index=[pd.Timestamp.now()])
        lib.append(sym_name, df, metadata={"version": i}, prune_previous_versions=True)

    print(lib.read(sym_name))
    print(lib.read(sym_name).data)
