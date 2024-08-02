/**
 * The first thing to know about are types. The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  i8 (byte)   Signed 8-bit integer
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 * Did you also notice that Thrift supports C style comments?
 */

// Just in case you were wondering... yes. We support simple C comments too.

/**
 * Thrift files can reference other Thrift files to include common struct
 * and service definitions. These are found using the current path, or by
 * searching relative to any paths specified with the -I compiler flag.
 *
 * Included objects are accessed using the name of the .thrift file as a
 * prefix. i.e. shared.SharedObject
 */

namespace py ps_service

typedef i32 MyInteger

enum Operation {
  PING = 1,
  REGISTER = 2,
  EXIST_MODEL = 3,
  CAN_PULL = 4,
  CAN_PUSH = 5,
  PULL_MODEL = 6,
  PUSH_GRAD = 7,
  PUSH_UPDATE = 8
}

struct Grad {
    1: string id,
    2: double learning_rate,
    3: optional i32 length,
    4: list<double> data,
    5: i32 n_iter,
    6: i32 worker_id
}

struct Update {
    1: string id,
    2: optional i32 length,
    3: list<double> data,
    4: i32 n_iter,
    5: i32 worker_id
}

struct Model {
    1: string id,
    2: optional i32 length,
    3: list<double> data,
}

exception InvalidOperation {
  1: i32 whatOp,
  2: string why
}

service ParameterServer {
    void ping() throws (1:InvalidOperation ex),
    void register_model(1:string id, 2:i32 length, 3:i32 parallelism) throws (1:InvalidOperation ex),
    bool exist_model(1:string id) throws (1:InvalidOperation ex),
    bool can_pull(1:string id, 2:i32 n_iter, 3:i32 worker_id) throws (1:InvalidOperation ex),
    bool can_push(1:string id, 2:i32 n_iter, 3:i32 worker_id) throws (1:InvalidOperation ex),
    Model pull_model(1:string id, 2:i32 n_iter, 3:i32 worker_id) throws (1:InvalidOperation ex),
    void push_grad(1:Grad g) throws (1:InvalidOperation ex),
    void push_update(1:Update u) throws (1:InvalidOperation ex),

    /**
    * This method has a oneway modifier. That means the client only makes
    * a request and does not listen for any response at all. Oneway methods
    * must be void.
    */
   oneway void zip()
}