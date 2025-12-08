import {
  _,
  Collection,
  Option,
  Enum,
  f32,
  f64,
  i32,
  i64,
  Struct,
  String,
  Tuple,
  u64,
  u8,
  u32,
  encode,
  decode,
  VALUE,
  VARIANT,
} from "bincode-ts";

const data = Uint8Array.from([
  252, 1, 0, 124, 77, 3, 2, 116, 115, 1, 0, 1, 3, 253, 0, 128, 222, 10, 38, 85,
  12, 0, 253, 0, 64, 141, 70, 78, 85, 12, 0, 253, 0, 0, 60, 130, 118, 85, 12, 0,
  5, 112, 114, 105, 99, 101, 4, 0, 4, 3, 251, 32, 123, 251, 16, 20, 251, 16, 30,
  4, 110, 117, 108, 108, 0, 0, 0, 0,
]);

const Item = Enum({
  Null: _(0),
  DateTime: _(1, Tuple(i64)),
  Int64: _(2, Tuple(i64)),
  Float64: _(3, Tuple(f64)),
  Decimal64: _(4, Tuple(u64)),
  String: _(5, Tuple(String)),
  Bool: _(6, Tuple(u8)),
  Int32: _(7, Tuple(i32)),
  Uint32: _(8, Tuple(u32)),
  UInt64: _(9, Tuple(u64)),
  Float32: _(10, Tuple(f32)),
  Bytes: _(11, Tuple(Collection(u8))),
  Decimal128: _(12, Tuple(u32, u32, u32, u32)),
});

const Series = Enum({
  Null: _(0, Tuple()),
  DateTime: _(1, Tuple(Collection(i64))),
  Int64: _(2, Tuple(Collection(i64))),
  Float64: _(3, Tuple(Collection(f64))),
  Decimal64: _(4, Tuple(Collection(u64))),
  String: _(5, Tuple(Collection(String))),
  Bool: _(6, Tuple(Collection(u8))),
  Int32: _(7, Tuple(Collection(i32))),
  UInt32: _(8, Tuple(Collection(u32))),
  UInt64: _(9, Tuple(Collection(u64))),
  Float32: _(10, Tuple(Collection(f32))),
  Bytes: _(11, Tuple(Collection(Collection(u8)))),
  Decimal128: _(12, Tuple(Collection(Tuple(u32, u32, u32, u32)))),
});

const DataType = Enum({
  Null: _(0),
  DateTime: _(1),
  Int64: _(2),
  Float64: _(3),
  Decimal64: _(4),
  String: _(5),
  Bool: _(6),
  Int32: _(7),
  UInt32: _(8),
  UInt64: _(9),
  Float32: _(10),
  Bytes: _(11),
  Decimal128: _(12),
});

const Field = Struct({
  name: String,
  kind: DataType,
  metadata: Option(Collection(Tuple(String, Item))),
  data: Series,
});

const Table = Struct({
  version: u32,
  columns: Collection(Field),
  metadata: Option(Collection(Tuple(String, Item))),
});

performance.mark("decode-start");
for (let i = 0; i < 1000; i++) {
  decode(Table, data.buffer, 0, {
    endian: "little",
    intEncoding: "variant",
  });
}
performance.mark("decode-end");

performance.measure("decode", "decode-start", "decode-end");

const measurements = performance.getEntriesByName("decode");
console.log(JSON.stringify(measurements, null, 2));
