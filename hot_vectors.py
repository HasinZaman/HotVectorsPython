import ctypes
import json
import struct
from ctypes import POINTER, c_ubyte, c_size_t, c_char_p

import socket
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from enum import Enum
from pathlib import Path

# Load the compiled library
lib_path = Path(__file__).parent / "rust_bytes_api.dll"  # Adjust extension per platform
lib = ctypes.CDLL(str(lib_path.resolve(strict=True)))

# Function bindings
lib.encode_request_cmd.argtypes = [c_char_p, POINTER(c_size_t)]
lib.encode_request_cmd.restype = POINTER(c_ubyte)

lib.decode_protocol_msg.argtypes = [POINTER(c_ubyte), c_size_t]
lib.decode_protocol_msg.restype = c_char_p

lib.free_bytes.argtypes = [ctypes.c_void_p]
lib.free_string.argtypes = [c_char_p]

def to_bytes(json_data: dict) -> bytes:
    """
    Converts a Python dictionary into a byte buffer using the Rust encoder.

    Args:
        json_data (dict): The JSON-compatible dictionary to encode.

    Returns:
        bytes: Encoded byte stream suitable for TCP transmission.
    """
    json_str = json.dumps(json_data).encode('utf-8')
    out_len = c_size_t()
    ptr = lib.encode_request_cmd(json_str, ctypes.byref(out_len))
    if not ptr:
        raise RuntimeError("Failed to encode in Rust")
    buf = ctypes.string_at(ptr, out_len.value)
    lib.free_bytes(ptr)
    return buf

def from_bytes(byte_data: bytes) -> dict:
    """
    Decodes a byte buffer using the Rust decoder into a Python dictionary.

    Args:
        byte_data (bytes): Encoded message buffer.

    Returns:
        dict: Decoded dictionary.
    """
    ptr = ctypes.cast(ctypes.create_string_buffer(byte_data), POINTER(c_ubyte))

    json_ptr = lib.decode_protocol_msg(ptr, len(byte_data))
    if not json_ptr:
        raise RuntimeError("Failed to decode in Rust")
    result = ctypes.string_at(json_ptr).decode('utf-8')
    return json.loads(result)

# =======================
# Data ID Wrappers
# =======================

@dataclass
class PartitionId():
    """Represents a UUID for a vector partition."""
    uuid:str

@dataclass
class ClusterId():
    """Identifies a cluster using a threshold and optional UUID."""
    threshold: float
    uuid: Optional[str]

@dataclass
class VectorId():
    """Represents a UUID for a specific vector."""
    uuid:str

# =======================
# Data Structures
# =======================

@dataclass
class Vector():
    """Represents a vector with an identifier and float list."""
    id: str
    vector: List[float]

@dataclass
class PartitionMeta():
    """Metadata for a partition."""
    id: str
    size: int
    centroid: List[float]
@dataclass
class ClusterMeta():
    """Metadata for a Cluster."""
    id: str
    size: int
    members: List[VectorId]
    centroid: List[float]



@dataclass
class IntraEdge():
    """Edge between two vectors within a partition."""
    uuid: PartitionId
@dataclass
class InterEdge():
    """Edge between vectors across different partitions."""
    pass

NIL_UUID = "00000000-0000-0000-0000-000000000000"

# =======================
# TCP Client
# =======================

@dataclass
class HotVectorClient:
    """
    Manages TCP connection and communication with the HotVectors DB.

    Attributes:
        host (str): Server IP.
        port (int): Server port.
        sock (Optional[socket.socket]): TCP socket connection.
    """
    host: str
    port: int
    sock: Optional[socket.socket] = None

    def __enter__(self):
        """Establishes TCP connection and validates with an 'Open' message."""
        self.sock = socket.create_connection((self.host, self.port))
        print(f"[TCP] Connected to {self.host}:{self.port}")

        # Wait for initial response from server (expected to be ProtocolMessage::Open)
        # try:
        print("checking if valid")
        initial = self._recv_message()
        decoded = from_bytes(initial)
        print(decoded)
        if decoded == "Open" and decoded is not None:
            print("[TCP] Received Open response from server ✅")
        else:
            raise RuntimeError(f"Unexpected initial message: {decoded}")
        # except Exception as e:
            # raise RuntimeError(f"Failed to receive initial Open response: {e}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the TCP connection."""
        if self.sock:
            self.sock.close()
            print("[TCP] Connection closed")

    def start_transaction(self):
        raise NotImplementedError()
    def end_transaction(self):
        raise NotImplementedError()
    
    def get_cluster_meta_data(self, threshold: float, uuid: Optional[str] = None) -> List[ClusterMeta]:
        data = {
            "Read": {
                "Meta": {
                    "source": {
                        "ClusterId": [
                            threshold,
                            uuid
                        ]
                    },
                    "dim_projection": None,
                    "attribute_projection": None,
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)
        
        response_bytes = self._recv_message() # should be Start

        meta_data: List[ClusterMeta] = []
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break

            cluster_id = response["Data"].get("ClusterId")
            if cluster_id is None:
                raise ValueError("Expected ClusterId")

            # Step 2: UInt (size)
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)
            size = response["Data"].get("UInt")
            if size is None:
                raise ValueError("Expected UInt")

            # Step 3: members
            members = []
            for i in range(size):
                response_bytes = self._recv_message()
                response = from_bytes(response_bytes)
           
                vector_info = response["Data"].get("Vector")
                if vector_info is None:
                    raise ValueError("Expected Vector")
                vector = vector_info[0]

                members.append(VectorId(vector))

            # Step 4: cluster centroid
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)
        
            vector_info = response["Data"].get("Vector")
            if vector_info is None:
                raise ValueError("Expected Vector")
            centroid = vector_info[1]

            meta_data.append(
                ClusterMeta(
                    cluster_id,
                    size,
                    members,
                    centroid
                )
            )
    
        return meta_data
    
    def get_partition_meta_data(self, uuid: Optional[str] = None) -> List[PartitionMeta]:
        """Fetches metadata for all available partitions."""
        data = {
            "Read": {
                "Meta": {
                    "source": {
                        "PartitionId": uuid if uuid is not None else NIL_UUID
                    },
                    "dim_projection": None,
                    "attribute_projection": None,
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)
        
        response_bytes = self._recv_message() # should be Start

        meta_data: List[PartitionMeta] = []
        while True:
            # Step 1: PartitionId
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)
            if response == "End":
                break
            partition_id = response["Data"].get("PartitionId")
            if partition_id is None:
                raise ValueError("Expected PartitionId")

            # Step 2: UInt (size)
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)
            size = response["Data"].get("UInt")
            if size is None:
                raise ValueError("Expected UInt")

            # Step 3: Vector (centroid)
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)
            vector_info = response["Data"].get("Vector")
            if vector_info is None:
                raise ValueError("Expected Vector")
            vector = vector_info[1]

            meta = PartitionMeta(id=partition_id, size=size, centroid=vector)
            meta_data.append(meta)

        return meta_data
    
    def get_graph_data(self, edge_type: IntraEdge | InterEdge) -> List[Tuple[float, VectorId, VectorId]] | List[Tuple[float, Tuple[PartitionId, VectorId], Tuple[PartitionId, VectorId]]]:
        """Fetches graph edge data depending on edge type."""
        match edge_type:
            case IntraEdge(partition_id):
                return self._get_intra_edges(partition_id)
            case InterEdge():
                return self._get_inter_edges()
            case _:
                raise NotImplementedError()

    def _get_intra_edges(self, partition_id: PartitionId) -> List[Tuple[float, VectorId, VectorId]]:
        data = {
            "Read": {
                "GraphEdges": {
                    "Intra": partition_id.uuid
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)
        
        response_bytes = self._recv_message()

        edges: List[Tuple[float, VectorId, VectorId]]  = []
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break

            data = response["Data"]["IntraEdge"]

            edges.append(
                (
                    data[0],
                    VectorId(data[1]),
                    VectorId(data[2])
                )
            )

        return edges
    
    def _get_inter_edges(self) -> List[Tuple[float, Tuple[PartitionId, VectorId], Tuple[PartitionId, VectorId]]]:
        data = {
            "Read": {
                "GraphEdges": "Inter"
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)

        response_bytes = self._recv_message()

        edges: List[Tuple[float, Tuple[PartitionId, VectorId], Tuple[PartitionId, VectorId]]]  = []
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break

            data = response["Data"]["InterEdge"]

            edges.append(
                (
                    data[0],
                    (
                        PartitionId(data[1][0]),
                        VectorId(data[1][1])
                    ),
                    (
                        PartitionId(data[2][0]),
                        VectorId(data[2][1])
                    )
                )
            )

        return edges

    def get_vectors(self, entity_id: VectorId | PartitionId | ClusterId) -> List[Vector]:
        """Gets vectors from either a partition or a cluster."""
        match entity_id:
            case VectorId(uuid):
                return self._get_vector_id_vectors(uuid)
            case PartitionId(uuid):
                return self._get_partition_vectors(uuid)
            case ClusterId(threshold, None):
                return self._get_all_cluster_vectors(threshold)
            case ClusterId(threshold, uuid):
                return self._get_cluster_id_vectors(threshold, uuid)
            case _:
                raise NotImplementedError()

    def _get_partition_vectors(self, uuid: str) -> List[Vector]:
        data = {
            "Read": {
                "Vectors": {
                    "source": {
                        "PartitionId": uuid
                    },
                    "dim_projection": None,
                    "attribute_projection": None
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)
        
        vectors = []

        response_bytes = self._recv_message() # should be Start

        # response_bytes = self._recv_message() # Partition Id (should verify)
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break
            data = response["Data"]["Vector"]

            vector = Vector(data[0], data[1])
            vectors.append(vector)

        return vectors

    def _get_vector_id_vectors(self, vector_id: str) -> List[Vector]:
        data = {
            "Read": {
                "Vectors": {
                    "source": {
                        "VectorId": vector_id
                    },
                    "dim_projection": None,
                    "attribute_projection": None
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)

        vectors = []

        response_bytes = self._recv_message()  # Start
        # response_bytes = self._recv_message()  # Vector ID (optional validation)
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break
            data = response["Data"]["Vector"]

            vector = Vector(data[0], data[1])
            vectors.append(vector)

        return vectors
    def _get_cluster_id_vectors(self, threshold: float, cluster_id: Optional[str] = None) -> List[Vector]:
        data = {
            "Read": {
                "Vectors": {
                    "source": {
                        "ClusterId": [
                            threshold,
                            cluster_id
                        ]
                    },
                    "dim_projection": None,
                    "attribute_projection": None
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)

        vectors = []

        response_bytes = self._recv_message()  # Start
        # response_bytes = self._recv_message()  # Cluster ID (optional validation)
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            if response == "End":
                break
            data = response["Data"]["Vector"]

            vector = Vector(data[0], data[1])
            vectors.append(vector)

        return vectors
    
    def _get_all_cluster_vectors(self, threshold: float) -> Dict[ClusterId, List[Vector]]:
        data = {
            "Read": {
                "ClusterVectors": {
                    "threshold": threshold
                }
            }
        }
        data = to_bytes(data)
        self._send_bytes(data)
        
        clusters = dict()
        response_bytes = self._recv_message() # should be Start

        current_cluster_id = from_bytes(self._recv_message())["Data"]["ClusterId"] # Partition Id (should verify)
        print(current_cluster_id)
        member_vectors = []
        while True:
            response_bytes = self._recv_message()
            response = from_bytes(response_bytes)

            match response:
                case "End":
                    clusters[current_cluster_id] = member_vectors
                    break
                case {"Data": {"Vector": [vector_id, None]}}:
                    member_vectors.append(vector_id)

                case {"Data": {"ClusterId": cluster_id}}:
                    clusters[current_cluster_id] = [*member_vectors]

                    member_vectors = []
                    current_cluster_id = cluster_id
                case _:
                    raise Exception()
    
        return clusters


    def send_vector(self, vector: List[float]) -> VectorId:
        """Inserts a new vector into the HotVectors DB and returns its ID."""
        data = {
            "InsertVector": vector
        }
        data = to_bytes(data)
        self._send_bytes(data)

        response_bytes = self._recv_message() # should be Start

        response_bytes = self._recv_message()
        response = from_bytes(response_bytes)

        response_bytes = self._recv_message() # should be End

        # TODO - Error checking
        return VectorId(response["Data"]["Vector"][0])
    
    def create_cluster(self, threshold: float) -> bool:
        """Requests the creation of a new clusterSet in the HotVectors DB."""
        data = {
            "CreateCluster": threshold
        }
        data = to_bytes(data)
        self._send_bytes(data)

        # TODO - Error checking
        start = from_bytes(self._recv_message())
        end = from_bytes(self._recv_message())

        return start == "Start" and end == "End"
    
    # =======================
    # Internal Communication Helpers
    # =======================

    def _send_bytes(self, data: bytes):
        if not self.sock:
            raise RuntimeError("Socket is not connected.")
        length_prefix = struct.pack('<I', len(data))
        self.sock.sendall(length_prefix+data)

    def _recv_message(self) -> bytes:
        if not self.sock:
            raise RuntimeError("Socket is not connected.")

        # Read 4-byte length prefix
        length_data = self._recv_exact(4)
        message_length = int.from_bytes(length_data, "little")
        return self._recv_exact(message_length)

    def _recv_exact(self, num_bytes: int) -> bytes:
        data = b""
        while len(data) < num_bytes:
            packet = self.sock.recv(num_bytes - len(data))
            if not packet:
                raise RuntimeError("Connection closed while reading data.")
            data += packet
        return data
    