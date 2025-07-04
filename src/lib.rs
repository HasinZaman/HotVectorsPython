use libc::{c_char, c_uchar, c_void, size_t};
use rkyv::{from_bytes, to_bytes, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::ptr;

type PartitionIdUUid = String;


#[derive(Archive, Debug, Serialize, RkyvSerialize, Deserialize, RkyvDeserialize, Clone)]
struct VectorSerial<A: Clone + Copy>(pub Vec<A>);

#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]
enum EdgeType {
    Intra(PartitionIdUUid),
    Inter,
}
#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]
enum SourceType<A: Archive> {
    VectorId(String),
    PartitionId(String),
    ClusterId(A, Option<String>),
}



#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]
enum ReadCmd<A: Archive> {
    // todo!() -> replace meta to be able to swap between
    //  -> Get all Partitions
    //  -> Get all Clusters
    //  -> filter to get a subset of data
    //  -> possible projections
    Meta {
        source: SourceType<A>,
    },

    Vectors {
        source: SourceType<A>,

        dim_projection: Option<usize>,
        
        attribute_projection: Option<(bool, bool)>
    },


    // Vector{ vector_id: VectorIdUUid },
    // PartitionVectors { partition_id: PartitionIdUUid },
    // ClusterVectors { threshold: A, cluster_id: Option<ClusterIdUUid>},

    GraphEdges(EdgeType),
}


#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]
enum RequestCmd<A: Clone + Copy + Archive> {
    StartTransaction,
    EndTransaction,
    Read(ReadCmd<A>),
    InsertVector(VectorSerial<A>),
    CreateCluster(A),
}

#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]

enum Data<A: Archive + Clone + Copy> {
    ClusterId(String),
    PartitionId(String),
    VectorId(String),

    Vector(Option<String>, Option<VectorSerial<A>>),

    InterEdge(A, (String, String), (String, String)),
    IntraEdge(A, String, String),

    UInt(usize)
}

#[derive(Archive, Debug, RkyvSerialize, Serialize, RkyvDeserialize, Deserialize, Clone)]
enum ProtocolMessage<A: Archive + Clone + Copy> {
    // Streaming?
    Start,
    End,

    //Organizational
    Pair(Data<A>, Data<A>),
    // Group(Vec<Data<A>>),
    Data(Data<A>),

    Open,
    TooManyConnections,
}

// Function A: JSON -> RequestCmd<f32> -> rkyv bytes
#[no_mangle]
pub extern "C" fn encode_request_cmd(json_ptr: *const c_char, out_len: *mut size_t) -> *mut c_uchar
{
    unsafe {
        if json_ptr.is_null() || out_len.is_null() {
            return ptr::null_mut();
        }

        let json_str = match CStr::from_ptr(json_ptr).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };

        let parsed: RequestCmd<f32> = match serde_json::from_str(json_str) {
            Ok(p) => p,
            Err(_) => return ptr::null_mut(),
        };

        let Ok(buffer) = to_bytes::<rancor::Error>(&parsed) else {
            return ptr::null_mut();
        };

        *out_len = buffer.len();
        let out_ptr: *mut u8 = libc::malloc(buffer.len()) as *mut c_uchar;
        if out_ptr.is_null() {
            return ptr::null_mut();
        }
        ptr::copy_nonoverlapping(buffer.as_ptr(), out_ptr, buffer.len());
        out_ptr
    }
}

#[no_mangle]
pub extern "C" fn decode_protocol_msg(bytes_ptr: *const c_uchar, length: size_t) -> *mut c_char {
    unsafe {
        if bytes_ptr.is_null() || length == 0 {
            println!("No bytes sent");
            return ptr::null_mut();
        }

        let bytes = std::slice::from_raw_parts(bytes_ptr, length);
        let Ok(archived) = from_bytes::<ProtocolMessage<f32>, rancor::Error>(bytes) else {
            println!("Couldn't convert bytes into message");
            return ptr::null_mut();
        };

        let Ok(json) = serde_json::to_string(&archived) else {
            println!("Couldn't convert message into json");
            return ptr::null_mut();
        };

        match CString::new(json) {
            Ok(val) => {
                val.into_raw()
            },
            Err(_err) => {
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_bytes(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            libc::free(ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr)); // frees memory
        }
    }
}