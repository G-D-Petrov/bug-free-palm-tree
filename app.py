from arcticdb import Arctic
import streamlit as st
from arcticdb.toolbox.library_tool import KeyType
from streamlit_agraph import agraph, Node, Edge, Config

ac = Arctic("lmdb://data/arcticdb")
lib = ac.get_library("vel_test")
syms = sorted(lib.list_symbols())
lib_tool = lib._nvs.library_tool()


config = Config(
    width=800,
    height=400,
    directed=True,
    nodeHighlightBehavior=True,
    staticGraph=True,
    physics=False,
    # **kwargs
)

# return_value = agraph(nodes=nodes, edges=edges, config=config)


class Index:
    def __init__(self, index):
        self.index = index
        try:
            # self.data = lib_tool.read_to_keys(index)
            self.data = 0
        except Exception as e:
            print(e)
            self.data = 0

    def __str__(self):
        return f"{self.index} -> {self.data}"

    def __repr__(self):
        return f"{self.index} -> {self.data}"


class Version:
    def __init__(self, ver):
        self.ver = ver
        ver_contents = lib_tool.read_to_keys(ver)
        if len(ver_contents) > 1:
            self.prev_ver = ver_contents[-1]
            self.index = ver_contents[:-1]  # THIS MIGHT BE WRONG
        else:
            self.prev_ver = None
            self.index = ver_contents

    def __str__(self):
        return f"{self.ver} -> {self.index}"

    def __repr__(self):
        return f"{self.ver} -> {self.index}"


def get_version_chain_iter(sym: str, num_versions: int):
    vers = lib_tool.find_keys_for_id(KeyType.VERSION, sym)
    vers = [Version(ver) for ver in vers]
    vers.reverse()
    # vers to nodes
    nodes = []
    edges = []

    x = 0
    for i, ver in enumerate(vers[:num_versions]):
        nodes.append(
            Node(
                id=ver.ver.version_id,
                label=ver.ver.version_id,
                size=10,
                x=i * 50,
                y=100,
            )
        )
    for ver in vers[:num_versions]:
        target = ver.prev_ver.version_id if ver.prev_ver else None
        edges.append(Edge(source=ver.ver.version_id, target=target))

    # return ref[0] if ref else None
    return agraph(nodes=nodes, edges=edges, config=config)


print(lib_tool.find_keys(KeyType.VERSION_REF))

# Set the title of the app
st.title("Velocity Day Demo")

selected_sym = st.sidebar.selectbox("Select a symbol to view:", syms)
selected_versions = st.sidebar.text_input(
    "Select the number of versions to display", 20
)
# Display the selected option
st.write(f"Version Chain for: {selected_sym}")

get_version_chain_iter(selected_sym, int(selected_versions))
