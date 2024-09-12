from arcticdb import Arctic
from arcticdb.toolbox.library_tool import KeyType

ac = Arctic("lmdb://data/arcticdb")
lib = ac.get_library("vel_test")
syms = sorted(lib.list_symbols())
lib_tool = lib._nvs.library_tool()
ver = lib_tool.find_keys_for_id(KeyType.VERSION, "sym_0")[-1]
print(lib_tool.read_to_keys(ver))
