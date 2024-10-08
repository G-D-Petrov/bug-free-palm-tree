from arcticdb import Arctic
import streamlit as st
from arcticdb.toolbox.library_tool import KeyType, key_to_props_dict
from streamlit_agraph import agraph, Node, Edge, Config
from show_node_details import ShowDetails

from arcticdb.version_store._normalization import FrameData
from arcticdb_ext.version_store import PythonOutputFrame
import pandas as pd


def read_to_df(lib_tool, key):
    segment = lib_tool.read_to_segment_in_memory(key)
    stream_desc = lib_tool.read_descriptor(key)
    field_names = [f.name for f in stream_desc.fields()]
    frame_data = FrameData.from_cpp(PythonOutputFrame(segment))
    cols = {}
    for idx, field_name in enumerate(field_names):
        cols[field_name] = frame_data.data[idx]
    return pd.DataFrame(cols, columns=field_names)


ac = Arctic("lmdb://data/arcticdb")
libs = ac.list_libraries()


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

        else:
            self.prev_ver = None
            self.content = ver_contents[0]

    def __str__(self):
        return f"{self.ver} -> {self.index}"

    def __repr__(self):
        return f"{self.ver} -> {self.index}"


VERSION = 0
TOMBSTONE = 1
INDEX = 2
id_to_key_mapping = {}


def key_to_node(key, x_index=None):
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
    elif key.type == KeyType.VERSION_REF:
        color = "yellow"
        y_index = 1
    else:
        raise ValueError(f"Unknown key type: {key.type} for key: {key}")

    x_index = x_index if x_index is not None else -key.version_id

    id_to_key_mapping[str(key)] = key

    return Node(
        id=str(key),
        label=str(key.version_id) if key.type != KeyType.VERSION_REF else "ref",
        color=color,
        shape="circle",
        x=x_index * 50,
        y=y_index * 50,
    )


def version_to_graph(ver: Version, index: int = None) -> list:
    nodes = []
    edges = []
    nodes.append(key_to_node(ver.ver, index))
    if ver.content:
        nodes.append(key_to_node(ver.content, index))
        edges.append(Edge(source=str(ver.ver), target=str(ver.content)))

    if ver.prev_ver:
        edges.append(Edge(source=str(ver.ver), target=str(ver.prev_ver)))
    return nodes, edges


def get_version_chain_iter(sym: str, num_versions: int):
    vers = lib_tool.find_keys_for_id(KeyType.VERSION, sym)
    # sort the vers first by version then by creation time
    vers = sorted(vers, key=lambda x: (x.version_id, x.creation_ts))

    vers = [Version(ver) for ver in vers]
    vers = vers[::-1]
    # vers to nodes
    nodes = []
    edges = []

    for i, ver in enumerate(vers[:num_versions]):
        ver_nodes, ver_edges = version_to_graph(ver, i)
        nodes.extend(ver_nodes)
        edges.extend(ver_edges)

    # return ref[0] if ref else None
    return agraph(nodes=nodes, edges=edges, config=config)


def read_ref_key(key, sym):
    df = read_to_df(lib_tool, key)
    df.index = pd.to_datetime(df.index)
    return lib_tool.dataframe_to_keys(df, sym)


def follow_ref_key(key):
    versions = []
    while key:
        versions.append(key)
        key = lib_tool.read_to_keys(key)[-1]
        if key.type != KeyType.VERSION:
            break
    return versions


def get_version_chain_ref(sym: str, num_versions: int):
    ref = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)
    keys = read_ref_key(ref[0], sym)
    ver_key = keys[-1]
    vers = follow_ref_key(keys[-1])

    vers = [Version(ver) for ver in vers]
    vers = vers
    # vers to nodes
    nodes = []
    edges = []

    nodes.append(key_to_node(ref[0], 0))
    edges.append(Edge(source=str(ref[0]), target=str(ver_key)))

    for i, ver in enumerate(vers[:num_versions]):
        ver_nodes, ver_edges = version_to_graph(ver, i + 1)
        nodes.extend(ver_nodes)
        edges.extend(ver_edges)

    # return ref[0] if ref else None
    return agraph(nodes=nodes, edges=edges, config=config)


def select_new_lib(lib_name):
    lib = ac.get_library(lib_name)
    lib_tool = lib._nvs.library_tool()
    syms = sorted(lib._nvs.list_symbols(all_symbols=True))
    return lib, lib_tool, syms


# Set the title of the app
st.title("Velocity Day Demo")

# Display the sidebar
selected_lib = st.sidebar.selectbox("Select a library to view:", libs)
lib = ac.get_library(selected_lib)
library_tool = lib._nvs.library_tool()
lib, lib_tool, syms = select_new_lib(selected_lib)

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
# st.write("Version Chain from all available version keys for the symbol")
# selected_node_iter = get_version_chain_iter(selected_sym, int(selected_versions))

st.write("Version Chain from the version ref key for the symbol")
selected_node_ref = get_version_chain_ref(selected_sym, int(selected_versions))


show_details = ShowDetails(st, lib_tool)
# Handle click events on nodes
if selected_node_ref:
    show_details.show_key_details(id_to_key_mapping[str(selected_node_ref)])
