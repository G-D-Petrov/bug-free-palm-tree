from arcticdb.toolbox.library_tool import KeyType, key_to_props_dict
import pandas as pd

class ShowDetails:
    def __init__(self, st, lib_tool):
        self.st = st
        self.lib_tool = lib_tool

    def show_key_data(self, key):
        key_data = key_to_props_dict(key)
        key_data["creation_ts"] = pd.Timestamp(key_data["creation_ts"])
        self.st.write("Variant key data:")
        self.st.json(key_data)

    def set_types_for_key_data(self, df):
        df["creation_ts"] = df["creation_ts"].apply(lambda x: pd.Timestamp(x))
        df["stream_id"] = df["stream_id"].apply(lambda x: str(x))
        df["key_type"] = df["key_type"].apply(lambda x: KeyType(x))

    def show_dataframe(self, key):
        if key.type == KeyType.TOMBSTONE or key.type == KeyType.TOMBSTONE_ALL:
            self.st.write("Tombstone keys are not written to storage and they don't have a respective dataframe or metadata.")
            return

        df = self.lib_tool.read_to_dataframe(key)
        if key.type != KeyType.TABLE_DATA:
            self.set_types_for_key_data(df)

        self.st.write("Stored dataframe:")
        self.st.dataframe(df)

    def show_metadata(self, key):
        metadata = self.lib_tool.read_metadata(key)
        self.st.write(f"metadata {metadata}")

    def show_key_details(self, selected_key):
        self.st.write(f"You clicked on {selected_key}")

        self.show_key_data(selected_key)
        self.show_dataframe(selected_key)
        self.show_metadata(selected_key)

