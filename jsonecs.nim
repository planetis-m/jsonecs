import
  jsonecs / [jsonnodes, slottables, heaparrays],
  std / [parsejson, streams, strutils, hashes]
export jsonnodes

const
  growthFactor = 2
  defaultInitialLen = 64
  defaultJObjectLen = 16

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
    indexes: seq[JsonNodeId]
    counter: int32
    parent: JsonNodeId

  Storage* = ref object
    signatures: SlotTable[set[JNodeKind]]
    # Atoms
    jbools: DArray[JBoolImpl]
    jints: DArray[JIntImpl]
    jfloats: DArray[JFloatImpl]
    jstrings: DArray[JStringImpl]
    # Mappings
    jnodes: DArray[JNodeImpl]

  JsonNode* = object
    id: JsonNodeId
    k: Storage

proc getJsonNodeId(x: Storage): JsonNodeId =
  result = x.signatures.incl({})

proc nextTry(h, maxHash: Hash): Hash {.inline.} =
  result = (h + 1) and maxHash

template maxHash(j): untyped = high(j.indexes)
template isFilled(n: JsonNodeId): bool = n != invalidId

proc mustRehash(length, counter: int): bool {.inline.} =
  assert(length > counter)
  result = (length * 2 < counter * 3) or (length - counter < 4)

proc enlarge(x: Storage; parent: JsonNodeId) =
  template jobject: untyped = x.jnodes[parent.idx]
  template jstring: untyped = x.jstrings[n.idx]

  var s = newSeq[JsonNodeId](jobject.indexes.len * growthFactor)
  swap(jobject.indexes, s)
  for i in 0..<s.len:
    let n = s[i]
    if isFilled(n):
      var h = hash(jstring) and maxHash(jobject)
      while isFilled(jobject.indexes[h]):
        h = nextTry(h, maxHash(jobject))
      jobject.indexes[h] = move s[i]

proc get(x: Storage, n: JsonNodeId, index: int): JsonNodeId {.inline.} =
  template jarray: untyped = x.jnodes[n.idx]
  if jarray.indexes.len > 0: result = jarray.indexes[index]
  else: result = invalidId

proc get(x: Storage, parent: JsonNodeId, key: string): JsonNodeId {.inline.} =
  template jobject: untyped = x.jnodes[parent.idx]
  template jstring: untyped = x.jstrings[n.idx]

  let origH = hash(key)
  var h = origH and maxHash(jobject)
  if jobject.indexes.len > 0:
    while true:
      let n = jobject.indexes[h]
      if not isFilled(n): break
      if jstring.str == key: return get(x, n, 0)
      h = nextTry(h, maxHash(jobject))
  result = invalidId

proc incl(x: Storage, parent, n: JsonNodeId, key: string): JsonNodeId =
  template jobject: untyped = x.jnodes[parent.idx]
  template jstring: untyped = x.jstrings[n.idx]

  let origH = hash(key)
  var h = origH and maxHash(jobject)
  if jobject.indexes.len > 0:
    while true:
      let n = jobject.indexes[h]
      if not isFilled(n): break
      if jstring.str == key: return n
      h = nextTry(h, maxHash(jobject))
    # Not found, we need to insert it:
    if mustRehash(jobject.indexes.len, jobject.counter):
      enlarge(x, n)
      # Recompute where to insert:
      h = origH and maxHash(jobject)
      while true:
        let n = jobject.indexes[h]
        if not isFilled(n): break
        h = nextTry(h, maxHash(jobject))
  else:
    setLen(jobject.indexes, defaultJObjectLen)
    h = origH and maxHash(jobject)
  result = n
  jobject.indexes[h] = n
  inc jobject.counter

template `?=`(name, value): bool = (let name = value; name != invalidId)
proc append(x: Storage, parent, n: JsonNodeId) =
  template jnode: untyped = x.jnodes[parent.idx]
  jnode.indexes.add n

proc delete*(x: Storage, n: JsonNodeId) =
  template jnode: untyped = x.jnodes[n.idx]
  if x.signatures[n] * {JArray, JObject} != {}:
    for j in jnode.indexes:
      delete(x, j)
  x.signatures.del(n)

template mixBody(has) =
  x.signatures[n].incl has

proc mustGrow[T](x: var DArray[T]; index: int): bool {.inline.} =
  result = x.len - index < 5

proc reserve[T](x: var DArray[T]; index: int) {.inline.} =
  if mustGrow(x, index): grow(x, x.len * growthFactor)

proc mixJNode(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JNode
  reserve(x.jnodes, n.idx)
  x.jnodes[n.idx] = JNodeImpl(parent: parent)
  if parent != invalidId:
    if x.signatures[parent] * {JArray, JKey} != {}: append(x, parent, n)

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

proc raiseDuplicateKeyError(key: string) {.noinline, noreturn.} =
  raise newException(KeyError, "key already exists in object: " & key)

proc mixJKey(x: Storage, n: JsonNodeId, s: sink string, parent = invalidId) =
  mixBody JKey
  mixJString(x, n, s)
  mixJNode(x, n, parent)
  if parent != invalidId:
    assert JObject in x.signatures[parent]
    if incl(x, parent, n, s) != n: # Duplicate keys!
      raiseDuplicateKeyError(s)

proc mixJObject(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JObject
  mixJNode(x, n, parent)

proc mixJArray(x: Storage, n: JsonNodeId, parent = invalidId) =
  mixBody JArray
  mixJNode(x, n, parent)

proc isNil*(x: JsonNode): bool {.inline.} = x.id == invalidId or x.k == nil

proc len*(x: JsonNode): int =
  if x.isNil: return
  let sign = x.k.signatures[x.id]
  if JArray in sign:
    result = x.k.jnodes[x.id.idx].indexes.len
  elif JObject in sign:
    result = x.k.jnodes[x.id.idx].counter
  else: discard

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
  template jarray: untyped = x.k.jnodes[x.id.idx]

  for child in jarray.indexes.items:
    yield JsonNode(id: child, k: x.k)

iterator pairs*(x: JsonNode): (lent string, JsonNode) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert x.isNil or JObject in x.k.signatures[x.id]
  template jobject: untyped = x.k.jnodes[x.id.idx]

  for child in jobject.indexes.items:
    if isFilled(child):
      template jstring: untyped = x.k.jstrings[child.idx]

      assert x.k.signatures[child] * {JKey, JString} == {JKey, JString}
      yield (jstring.str, JsonNode(id: get(x.k, child, 0), k: x.k))

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
    if x.k.isNil or JObject notin x.k.signatures[resultId]: return JsonNode(id: invalidId)
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
    if x.k.isNil or JArray notin x.k.signatures[resultId]: return JsonNode(id: invalidId)
    block searchLoop:
      resultId = get(x.k, resultId, j)
      if resultId != invalidId:
        break searchLoop
      return JsonNode(id: invalidId)
  result = JsonNode(id: resultId, k: x.k)

proc parseJson(x: Storage; p: var JsonParser; rawIntegers, rawFloats: bool;
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
