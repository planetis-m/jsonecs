type
  JsonNode* = distinct JsonNodeImpl
  JsonNodeImpl* = uint32

const
  versionBits = 6
  versionMask = 1 shl versionBits - 1
  indexBits = sizeof(JsonNodeImpl) * 8 - versionBits
  indexMask = 1 shl indexBits - 1
  invalidId* = JsonNode(indexMask) # a sentinel value to represent an invalid entity
  maxJsonNodes* = indexMask

template idx*(n: JsonNode): int = n.int and indexMask
template version*(n: JsonNode): JsonNodeImpl = n.JsonNodeImpl shr indexBits and versionMask
template toJsonNode*(idx, v: JsonNodeImpl): JsonNode = JsonNode(v shl indexBits or idx)

proc `==`*(a, b: JsonNode): bool {.borrow.}
proc `$`*(n: JsonNode): string =
  "JsonNode(i: " & $n.idx & ", v: " & $n.version & ")"
