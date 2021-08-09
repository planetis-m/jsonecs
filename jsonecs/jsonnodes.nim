type
  JsonNodeId* = distinct JsonNodeIdImpl
  JsonNodeIdImpl* = uint32

const
  versionBits = 6
  versionMask = 1 shl versionBits - 1
  indexBits = sizeof(JsonNodeIdImpl) * 8 - versionBits
  indexMask = 1 shl indexBits - 1
  invalidId* = JsonNodeId(indexMask) # a sentinel value to represent an invalid entity
  maxJsonNodes* = indexMask

template idx*(n: JsonNodeId): int32 = n.int32 and indexMask
template version*(n: JsonNodeId): JsonNodeIdImpl = n.JsonNodeIdImpl shr indexBits and versionMask
template toJsonNodeId*(idx, v: JsonNodeIdImpl): JsonNodeId = JsonNodeId(v shl indexBits or idx)

proc `==`*(a, b: JsonNodeId): bool {.borrow.}
proc `$`*(n: JsonNodeId): string =
  "JsonNodeId(i: " & $n.idx & ", v: " & $n.version & ")"
