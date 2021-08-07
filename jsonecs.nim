import
  jsonecs / [jsonnodes, slottables, heaparrays],
  std / [parsejson, streams, strutils, hashes]
export jsonnodes

const
  growthFactor = 2
  defaultInitialLen = 64

type
  JNodeKind* = enum
    JNull
    JBool
    JInt
    JFloat
    JString
    JObject
    JArray
    JKey
    JRawNumber
    JNode

  JBoolImpl = object
    bval: bool

  JIntImpl = object
    num: BiggestInt

  JFloatImpl = object
    fnum: float

  JStringImpl = object
    str: string
    isUnquoted: bool

  JNodeImpl = object
    head, tail: JsonNodeId
    next: JsonNodeId
    parent: JsonNodeId

  JKeyImpl = object
    hcode: Hash

  Storage* = ref object
    signatures: SlotTable[set[JNodeKind]]
    # Atoms
    jbools: DArray[JBoolImpl]
    jints: DArray[JIntImpl]
    jfloats: DArray[JFloatImpl]
    jstrings: DArray[JStringImpl]
    jkeys: DArray[JKeyImpl]
    # Mappings
    jnodes: DArray[JNodeImpl]

  JsonNode* = object
    id: JsonNodeId
    k: Storage

proc getJsonNodeId(x: Storage): JsonNodeId =
  result = x.signatures.incl({})

iterator queryAll(x: Storage, parent: JsonNodeId,
    query: set[JNodeKind]): JsonNodeId =
  template hierarchy: untyped = x.jnodes[n.idx]

  var frontier = @[parent]
  while frontier.len > 0:
    let n = frontier.pop()
    if x.signatures[n] * query == query:
      yield n
    var childId = hierarchy.head
    while childId != invalidId:
      template childHierarchy: untyped = x.jnodes[childId.idx]

      frontier.add(childId)
      childId = childHierarchy.next

template `?=`(name, value): bool = (let name = value; name != invalidId)
proc append(x: Storage, parentId, n: JsonNodeId) =
  template hierarchy: untyped = x.jnodes[n.idx]
  template parent: untyped = x.jnodes[parentId.idx]
  template tailSibling: untyped = x.jnodes[tailSiblingId.idx]

  hierarchy.next = invalidId
  if tailSiblingId ?= parent.tail:
    assert tailSibling.next == invalidId
    tailSibling.next = n
  parent.tail = n
  if parent.head == invalidId: parent.head = n

proc removeNode(x: Storage, n: JsonNodeId) =
  template hierarchy: untyped = x.jnodes[n.idx]
  template parent: untyped = x.jnodes[parentId.idx]
  template tailSibling: untyped = x.jnodes[tailSiblingId.idx]

  if parentId ?= hierarchy.parent:
    if n == parent.head:
      parent.head = hierarchy.next
      if tailSiblingId ?= parent.tail:
        if tailSibling.next == n:
          tailSibling.next = parent.head

proc delete*(x: Storage, n: JsonNodeId) =
  ## Deletes `x[key]`.
  var toDelete: seq[JsonNodeId]
  for entity in queryAll(x, n, {JNode}):
    removeNode(x, entity)
    toDelete.add(entity)
  for entity in toDelete.items:
    x.signatures.del(entity)

template mixBody(has) =
  x.signatures[n].incl has

proc mustGrow[T](x: var DArray[T]; index: int): bool {.inline.} =
  result = x.len - index < 5

proc reserve[T](x: var DArray[T]; index: int) {.inline.} =
  if mustGrow(x, index): grow(x, x.len * growthFactor)

proc mixJNode(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JNode
  reserve(x.jnodes, n.idx)
  x.jnodes[n.idx] = JNodeImpl(
    head: invalidId, tail: invalidId, next: invalidId,
    parent: parent)
  if parent != invalidId: append(x, parent, n)

proc mixJNull(x: Storage, n: JsonNodeId) =
  mixBody JNull

proc mixJBool(x: Storage, n: JsonNodeId, b: bool) =
  mixBody JBool
  reserve(x.jbools, n.idx)
  x.jbools[n.idx] = JBoolImpl(bval: b)

proc mixJInt(x: Storage, n: JsonNodeId, i: BiggestInt) =
  mixBody JInt
  reserve(x.jints, n.idx)
  x.jints[n.idx] = JIntImpl(num: i)

proc mixJFloat(x: Storage, n: JsonNodeId, f: float) =
  mixBody JFloat
  reserve(x.jfloats, n.idx)
  x.jfloats[n.idx] = JFloatImpl(fnum: f)

proc mixJString(x: Storage, n: JsonNodeId, s: sink string) =
  mixBody JString
  reserve(x.jstrings, n.idx)
  x.jstrings[n.idx] = JStringImpl(str: s)

proc mixJRawNumber(x: Storage, n: JsonNodeId, s: sink string) =
  mixBody JString
  mixBody JRawNumber
  reserve(x.jstrings, n.idx)
  x.jstrings[n.idx] = JStringImpl(str: s, isUnquoted: true)

proc mixJKey(x: Storage, n: JsonNodeId, s: sink string, parent = invalidId) =
  mixBody JKey
  reserve(x.jkeys, n.idx)
  x.jkeys[n.idx] = JKeyImpl(hcode: hash(s))
  mixJString(x, n, s)
  mixJNode(x, n, parent)

proc mixJObject(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JObject
  mixJNode(x, n, parent)

proc mixJArray(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JArray
  mixJNode(x, n, parent)

proc isNil*(x: JsonNode): bool {.inline.} = x.id == invalidId

proc kind*(x: JsonNode): JNodeKind =
  assert not x.isNil
  let sign = x.k.signatures[x.id]
  if JNull in sign:
    result = JNull
  elif JBool in sign:
    result = JBool
  elif JFloat in sign:
    result = JFloat
  elif JInt in sign:
    result = JInt
  elif JString in sign:
    result = JString
  elif JObject in sign:
    result = JObject
  elif JArray in sign:
    result = JArray
  else:
    result = JNull

iterator items*(x: JsonNode): JsonNode =
  ## Iterator for the items of `x`. `x` has to be a JArray.
  assert x.isNil or JArray in x.k.signatures[x.id]
  template hierarchy: untyped = x.k.jnodes[x.id.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.k.jnodes[childId.idx]

    yield JsonNode(id: childId, k: x.k)
    childId = childHierarchy.next

iterator pairs*(x: JsonNode): (lent string, JsonNode) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert x.isNil or JObject in x.k.signatures[x.id]
  template hierarchy: untyped = x.k.jnodes[x.id.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.k.jnodes[childId.idx]
    template jstring: untyped = x.k.jstrings[childId.idx]

    assert x.k.signatures[childId] * {JKey, JString} == {JKey, JString}
    yield (jstring.str, JsonNode(id: childHierarchy.head, k: x.k))
    childId = childHierarchy.next

proc getStr*(x: JsonNode, default: string = ""): string =
  ## Retrieves the string value of a `JString`.
  ##
  ## Returns `default` if `x` is not a `JString`.
  if x.isNil or JString in x.k.signatures[x.id]: result = x.k.jstrings[x.id.idx].str
  else: result = default

proc getInt*(x: JsonNode, default: int = 0): int =
  ## Retrieves the int value of a `JInt`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if x.isNil or JInt in x.k.signatures[x.id]: result = int(x.k.jints[x.id.idx].num)
  else: result = default

proc getBiggestInt*(x: JsonNode, default: BiggestInt = 0): BiggestInt =
  ## Retrieves the BiggestInt value of a `JInt`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if x.isNil or JInt in x.k.signatures[x.id]: result = x.k.jints[x.id.idx].num
  else: result = default

proc getFloat*(x: JsonNode, default: float = 0.0): float =
  ## Retrieves the float value of a `JFloat`.
  ##
  ## Returns `default` if `x` is not a `JFloat` or `JInt`, or if `x` is nil.
  if x.isNil: return default
  let sign = x.k.signatures[x.id]
  if JFloat in sign:
    result = x.k.jfloats[x.id.idx].fnum
  elif JInt in sign:
    result = float(x.k.jints[x.id.idx].num)
  else:
    result = default

proc getBool*(x: JsonNode, default: bool = false): bool =
  ## Retrieves the bool value of a `JBool`.
  ##
  ## Returns `default` if `n` is not a `JBool`, or if `n` is nil.
  if x.isNil or JBool in x.k.signatures[x.id]: result = x.k.jbools[x.id.idx].bval
  else: result = default

proc raiseKeyError(key: string) {.noinline, noreturn.} =
  raise newException(KeyError, "key not found in object: " & key)

proc raiseIndexDefect {.noinline, noreturn.} =
  raise newException(IndexDefect, "index out of bounds")

proc get(x: Storage, n: JsonNodeId, key: string): JsonNodeId {.inline.} =
  template hierarchy: untyped = x.jnodes[n.idx]

  var childId = hierarchy.head
  let h = hash(key)
  while childId != invalidId:
    template childHierarchy: untyped = x.jnodes[childId.idx]
    template jkey: untyped = x.jkeys[childId.idx]
    template jstring: untyped = x.jstrings[childId.idx]

    assert x.signatures[childId] * {JKey, JString} == {JKey, JString}
    if jkey.hcode == h and jstring.str == key:
      return childHierarchy.head
    childId = childHierarchy.next
  result = invalidId

proc get(x: Storage, n: JsonNodeId, index: int): JsonNodeId {.inline.} =
  template hierarchy: untyped = x.jnodes[n.idx]

  var childId = hierarchy.head
  var i = index
  while childId != invalidId:
    template childHierarchy: untyped = x.jnodes[childId.idx]

    if i == 0: return childId
    dec i
    childId = childHierarchy.next
  result = invalidId

proc `[]`*(x: JsonNode, key: string): JsonNode {.inline.} =
  ## Gets a field from a `JObject`, which must not be nil.
  ## If the value at `key` does not exist, raises KeyError.
  assert x.isNil or JObject in x.k.signatures[x.id]
  let id = get(x.k, x.id, key)
  if id != invalidId: result = JsonNode(id: id, k: x.k)
  else: raiseKeyError(key)

proc `[]`*(x: JsonNode, index: int): JsonNode {.inline.} =
  ## Gets the node at `index` in an Array. Result is undefined if `index`
  ## is out of bounds, but as long as array bound checks are enabled it will
  ## result in an exception.
  assert x.isNil or JArray in x.k.signatures[x.id]
  let id = get(x.k, x.id, index)
  if id != invalidId: result = JsonNode(id: id, k: x.k)
  else: raiseIndexDefect()

proc contains*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `n`.
  assert x.isNil or JObject in x.k.signatures[x.id]
  result = get(x.k, x.id, key) != invalidId

proc hasKey*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `x`.
  result = x.contains(key)

proc `{}`*(x: JsonNode, keys: varargs[string]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## keys do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an object.
  var resultId = x.id
  for kk in keys:
    if x.isNil or JObject notin x.k.signatures[resultId]: return JsonNode(id: invalidId)
    block searchLoop:
      resultId = get(x.k, resultId, kk)
      if resultId != invalidId:
        break searchLoop
      return JsonNode(id: invalidId)
  result = JsonNode(id: resultId, k: x.k)

proc `{}`*(x: JsonNode, indexes: varargs[int]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## indexes do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an array.
  var resultId = x.id
  for j in indexes:
    if x.isNil or JArray notin x.k.signatures[resultId]: return JsonNode(id: invalidId)
    block searchLoop:
      resultId = get(x.k, resultId, j)
      if resultId != invalidId:
        break searchLoop
      return JsonNode(id: invalidId)
  result = JsonNode(id: resultId, k: x.k)

proc parseJson(x: var Storage; p: var JsonParser; rawIntegers, rawFloats: bool;
      parent: JsonNodeId): JsonNodeId =
  case p.tok
  of tkString:
    result = getJsonNodeId(x)
    mixJString(x, result, p.a)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkInt:
    result = getJsonNodeId(x)
    if rawIntegers:
      mixJRawNumber(x, result, p.a)
    else:
      try:
        mixJInt(x, result, parseBiggestInt(p.a))
      except ValueError:
        mixJRawNumber(x, result, p.a)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkFloat:
    result = getJsonNodeId(x)
    if rawFloats:
      mixJRawNumber(x, result, p.a)
    else:
      try:
        mixJFloat(x, result, parseFloat(p.a))
      except ValueError:
        mixJRawNumber(x, result, p.a)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkTrue:
    result = getJsonNodeId(x)
    mixJBool(x, result, true)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkFalse:
    result = getJsonNodeId(x)
    mixJBool(x, result, false)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkNull:
    result = getJsonNodeId(x)
    mixJNull(x, result)
    mixJNode(x, result, parent)
    discard getTok(p)
  of tkCurlyLe:
    result = getJsonNodeId(x)
    mixJObject(x, result, parent)
    discard getTok(p)
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raiseParseErr(p, "string literal as key")
      let key = getJsonNodeId(x)
      mixJKey(x, key, p.a, result)
      discard getTok(p)
      eat(p, tkColon)
      discard parseJson(x, p, rawIntegers, rawFloats, key)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkCurlyRi)
  of tkBracketLe:
    result = getJsonNodeId(x)
    mixJArray(x, result, parent)
    discard getTok(p)
    while p.tok != tkBracketRi:
      discard parseJson(x, p, rawIntegers, rawFloats, result)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkBracketRi)
  of tkError, tkCurlyRi, tkBracketRi, tkColon, tkComma, tkEof:
    raiseParseErr(p, "{")

proc parseJson*(s: Stream, filename: string = "";
    rawIntegers = false, rawFloats = false): JsonNode =
  ## Parses from a stream `s` into a `JsonNode`. `filename` is only needed
  ## for nice error messages.
  ## If `s` contains extra data, it will raise `JsonParsingError`.
  var p: JsonParser
  open(p, s, filename)
  try:
    discard getTok(p)
    result.k = Storage(
        signatures: newSlotTableOfCap[set[JNodeKind]](defaultInitialLen),
        jbools: newDArray[JBoolImpl](defaultInitialLen),
        jints: newDArray[JIntImpl](defaultInitialLen),
        jfloats: newDArray[JFloatImpl](defaultInitialLen),
        jstrings: newDArray[JStringImpl](defaultInitialLen),
        jkeys: newDArray[JKeyImpl](defaultInitialLen),
        jnodes: newDArray[JNodeImpl](defaultInitialLen)
      )
    result.id = parseJson(result.k, p, rawIntegers, rawFloats, invalidId)
    eat(p, tkEof)
  finally:
    close(p)

proc parseJson*(buffer: string;
    rawIntegers = false, rawFloats = false): JsonNode =
  ## Parses JSON from `buffer`.
  ## If `buffer` contains extra data, it will raise `JsonParsingError`.
  parseJson(newStringStream(buffer), "input", rawIntegers, rawFloats)

proc parseFile*(filename: string): JsonNode =
  ## Parses `file` into a `JsonNode`.
  ## If `file` contains extra data, it will raise `JsonParsingError`.
  var stream = newFileStream(filename, fmRead)
  if stream == nil:
    raise newException(IOError, "cannot read from file: " & filename)
  result = parseJson(stream, filename)

when isMainModule:
  block:
    let testJson = parseJson"""{ "a": [1, 2, {"key": [4, 5]}, 4]}"""
    for (key, node1) in pairs(testJson):
      echo key, ": ", node1.kind
      for node2 in items(node1):
        echo node2.getInt()
  block:
    let testJson = parseJson"""{"name": "Isaac", "books": ["Robot Dreams"],
        "details": {"age": 35, "pi": 3.1415}}"""
    for (key, node1) in pairs(testJson):
      echo key, ": ", node1.kind
    echo testJson{"details", "age"}.getInt()
