from arcticdb import Arctic
import streamlit as st
from arcticdb.toolbox.library_tool import KeyType
from streamlit_agraph import agraph, Node, Edge, Config

ac = Arctic("lmdb://data/arcticdb")
lib = ac.get_library("vel_test")
syms = sorted(lib.list_symbols())
lib_tool = lib._nvs.library_tool()


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
        print(ver.version_id)
        print(ver_contents)
        if len(ver_contents) > 1:
            self.prev_ver = ver_contents[-1]
            self.content = ver_contents[0]  # THIS MIGHT BE WRONG
            if self.content.type == KeyType.TABLE_INDEX:
                self.type = INDEX
            elif (
                self.content.type == KeyType.TOMBSTONE
                or self.content.type == KeyType.TOMBSTONE_ALL
            ):
                self.type = TOMBSTONE
            else:
                self.type = VERSION
            if len(ver_contents) > 2:
                print(f"{ver} has more than 2 keys")
            # print(ver.version_id)
            # print(self.index[0].type)
            # print(self.index[0].version_id)
        else:
            self.prev_ver = None
            self.index = ver_contents

    def __str__(self):
        return f"{self.ver} -> {self.index}"

    def __repr__(self):
        return f"{self.ver} -> {self.index}"


VERSION = 0
TOMBSTONE = 1
INDEX = 2


def key_to_node(key):
    color = "black"
    y_index = 100
    if key.type == KeyType.TABLE_INDEX:
        color = "blue"
        y_index = 0
    elif key.type == KeyType.TOMBSTONE or key.type == KeyType.TOMBSTONE_ALL:
        color = "red"
        y_index = 2
    elif key.type == KeyType.VERSION:
        color = "green"
        y_index = 1
    else:
        raise ValueError(f"Unknown key type: {key.type} for key: {key}")

    return Node(
        id=str(key),
        # label=ver.ver.version_id,
        color=color,
        size=10,
        x=-key.version_id * 50,
        y=y_index * 50,
    )


def version_to_graph(ver: Version) -> list:
    nodes = []
    edges = []
    nodes.append(key_to_node(ver.ver))
    if ver.content:
        nodes.append(key_to_node(ver.content))
        edges.append(Edge(source=str(ver.content), target=str(ver.ver)))

    if ver.prev_ver:
        edges.append(Edge(source=str(ver.ver), target=str(ver.prev_ver)))
    return nodes, edges


def get_version_chain_iter(sym: str, num_versions: int):
    vers = lib_tool.find_keys_for_id(KeyType.VERSION, sym)
    vers = [Version(ver) for ver in vers]
    vers = vers[::-1]
    # vers to nodes
    nodes = []
    edges = []

    for i, ver in enumerate(vers[:num_versions]):
        ver_nodes, ver_edges = version_to_graph(ver)
        nodes.extend(ver_nodes)
        edges.extend(ver_edges)

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


config = Config(
    width=800,
    height=400,
    directed=True,
    nodeHighlightBehavior=True,
    staticGraphWithDragAndDrop=False,  # Allow graph movement for dynamic interaction
    staticGraph=True,  # Allows click events
    physics=False,
)

# Display the graph
selected_node = get_version_chain_iter(selected_sym, int(selected_versions))

# Handle click events on nodes
if selected_node:
    st.write(f"You clicked on {selected_node}")
