import ctypes
import json
import struct
from ctypes import POINTER, c_ubyte, c_size_t, c_char_p

import socket
from dataclasses import dataclass, field
from typing import Optional, List

# Load the compiled library
lib = ctypes.CDLL('./rust_bytes_api.dll')  # Adjust as needed

# Function bindings
lib.encode_request_cmd.argtypes = [c_char_p, POINTER(c_size_t)]
lib.encode_request_cmd.restype = POINTER(c_ubyte)

lib.decode_protocol_msg.argtypes = [POINTER(c_ubyte), c_size_t]
lib.decode_protocol_msg.restype = c_char_p

lib.free_bytes.argtypes = [ctypes.c_void_p]
lib.free_string.argtypes = [c_char_p]

def to_bytes(json_data: dict) -> bytes:
    json_str = json.dumps(json_data).encode('utf-8')
    out_len = c_size_t()
    ptr = lib.encode_request_cmd(json_str, ctypes.byref(out_len))
    if not ptr:
        raise RuntimeError("Failed to encode in Rust")
    buf = ctypes.string_at(ptr, out_len.value)
    lib.free_bytes(ptr)
    return buf

def from_bytes(byte_data: bytes) -> dict:
    ptr = ctypes.cast(ctypes.create_string_buffer(byte_data), POINTER(c_ubyte))

    json_ptr = lib.decode_protocol_msg(ptr, len(byte_data))
    if not json_ptr:
        raise RuntimeError("Failed to decode in Rust")
    result = ctypes.string_at(json_ptr).decode('utf-8')
    return json.loads(result)

@dataclass
class PartitionId():
    uuid:str
    
@dataclass
class ClusterId():
    uuid:str
@dataclass
class VectorId():
    uuid:str


@dataclass
class TcpStreamClient:
    host: str
    port: int
    sock: Optional[socket.socket] = None

    def __enter__(self):
        self.sock = socket.create_connection((self.host, self.port))
        print(f"[TCP] Connected to {self.host}:{self.port}")

        # Wait for initial response from server (expected to be ProtocolMessage::Open)
        # try:
        print("checking if valid")
        initial = self._recv_message()
        decoded = from_bytes(initial)
        if decoded == "Open" and decoded is not None:
            print("[TCP] Received Open response from server ✅")
        else:
            raise RuntimeError(f"Unexpected initial message: {decoded}")
        # except Exception as e:
            # raise RuntimeError(f"Failed to receive initial Open response: {e}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sock:
            self.sock.close()
            print("[TCP] Connection closed")

    def start_transaction(self):
        raise NotImplementedError()
    def end_transaction(self):
        raise NotImplementedError()
    
    def get_meta_data(self):
        raise NotImplementedError()

    def get_vectors(self, entity_id: PartitionId | ClusterId):
        raise NotImplementedError()


    def send_vector(self, vector: List[float]) -> VectorId:
        data = {
            "InsertVector": vector
        }
        data = to_bytes(data)
        self._send_bytes(data)

        response_bytes = self._recv_message()

        response_bytes = self._recv_message() # should be start
        response = from_bytes(response_bytes)
        response_bytes = self._recv_message() # should be end

        return response
    def create_cluster(self, threshold):
        raise NotImplementedError()
        
    def get_partitions(self) -> str:
        raise NotImplementedError()

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

# Example Usage
if __name__ == "__main__":
    import random as r
    with HotVectorClient('127.0.0.1', 3000) as client:
        print(client.send_vector([r.random(),r.random()]))