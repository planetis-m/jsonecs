import
  jsonecs / [jsonnodeids, slottables, heaparrays],
  std / [parsejson, streams, strutils, hashes]
export jsonnodeids

{.experimental: "strictFuncs".}

const
  growthFactor = 2
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
    JRawNumber # isUnquoted
    JNode

  JBoolImpl = object
    bval: bool

  JIntImpl = object
    num: BiggestInt

  JFloatImpl = object
    fnum: float

  JStringImpl = object
    str: string

  JNodeImpl = object
    case kind: JNodeKind
    of JObject:
      keys: seq[JsonNodeId]
      counter: int32
    of JArray:
      indexes: seq[JsonNodeId]
    of JKey:
      hcode: Hash
      son: JsonNodeId
    else: discard

  Storage* = object
    signatures: SlotTable[set[JNodeKind]]
    # Atoms
    jbools: SecTable[JBoolImpl]
    jints: SecTable[JIntImpl]
    jfloats: SecTable[JFloatImpl]
    jstrings: SecTable[JStringImpl]
    # Mappings
    jnodes: SecTable[JNodeImpl]

  JsonNode* = object
    id: JsonNodeId
    k: ref Storage

proc getJsonNodeId(x: var Storage): JsonNodeId =
  result = x.signatures.incl({JNode})

proc nextTry(h, maxHash: Hash): Hash {.inline.} =
  result = (h + 1) and maxHash

template maxHash(j): untyped = high(j.keys)
proc isFilled(x: Storage, n: JsonNodeId): bool {.inline.} = n in x.signatures
proc isEmpty(x: Storage, n: JsonNodeId): bool {.inline.} = n notin x.signatures

proc mustRehash(length, counter: int): bool {.inline.} =
  assert length > counter
  result = (length * 2 < counter * 3) or (length - counter < 4)

proc enlarge(x: var Storage; parent: JsonNodeId) =
  template jobject: untyped = x.jnodes[parent]
  template jkey: untyped = x.jnodes[n]

  var s: seq[JsonNodeId]
  grow(s, jobject.keys.len * growthFactor, invalidId)
  swap(jobject.keys, s)
  for i in 0..<s.len:
    let n = s[i]
    if isFilled(x, n):
      var h = jkey.hcode and maxHash(jobject)
      while isFilled(x, jobject.keys[h]):
        h = nextTry(h, maxHash(jobject))
      jobject.keys[h] = s[i]

proc get(x: Storage, parent: JsonNodeId, key: string): JsonNodeId =
  template jobject: untyped = x.jnodes[parent]
  template jstring: untyped = x.jstrings[n]
  template jkey: untyped = x.jnodes[n]

  let origH = hash(key)
  var h = origH and maxHash(jobject)
  if jobject.keys.len > 0:
    while true:
      let n = jobject.keys[h]
      if not isFilled(x, n): break
      if jkey.hcode == origH and jstring.str == key: return n
      h = nextTry(h, maxHash(jobject))
  result = invalidId

proc getOrIncl(x: var Storage, parent, n: JsonNodeId, key: string): JsonNodeId =
  template jobject: untyped = x.jnodes[parent]
  template jstring: untyped = x.jstrings[n]
  template jkey: untyped = x.jnodes[n]

  let origH = hash(key)
  var h = origH and maxHash(jobject)
  if jobject.keys.len > 0:
    while true:
      let n = jobject.keys[h]
      if not isFilled(x, n): break
      if jkey.hcode == origH and jstring.str == key: return n
      h = nextTry(h, maxHash(jobject))
    # Not found, we need to insert it:
    if mustRehash(jobject.keys.len, jobject.counter):
      enlarge(x, n)
      # Recompute where to insert:
      h = origH and maxHash(jobject)
      while true:
        let n = jobject.keys[h]
        if not isFilled(x, n): break
        h = nextTry(h, maxHash(jobject))
  else:
    jobject.keys.grow(defaultJObjectLen, invalidId)
    h = origH and maxHash(jobject)
  result = n
  jobject.keys[h] = n
  inc jobject.counter

proc exclAndGet(x: var Storage; parent: JsonNodeId, key: string): JsonNodeId =
  # Impl adapted from `tableimpl.delImplIdx`
  template jobject: untyped = x.jnodes[parent]
  template jstring: untyped = x.jstrings[n]
  template jkey: untyped = x.jnodes[n]

  let origH = hash(key)
  let msk = maxHash(jobject)
  var h = origH and msk
  if jobject.keys.len > 0:
    while true:
      let n = jobject.keys[h]
      if not isFilled(x, n): break
      if jkey.hcode == origH and jstring.str == key:
        result = n
        dec jobject.counter
        while true: # KnuthV3 Algo6.4R adapted for i=i+1 instead of i=i-1
          var j = h # The correctness of this depends on (h+1) in nextTry,
          var r = j # though may be adaptable to other simple sequences.
          jobject.keys[h] = invalidId # mark current EMPTY
          while true:
            h = nextTry(h, msk) # increment mod table size
            let n = jobject.keys[h]
            if isEmpty(x, n): # end of collision cluster; So all done
              return
            r = jkey.hcode and msk # "home" location of key@i
            if not ((h >= r and r > j) or (r > j and j > h) or (j > h and h >= r)):
              break
          jobject.keys[j] = jobject.keys[h]
      h = nextTry(h, msk)
  result = invalidId

template mixBody(has) =
  x.signatures[n].incl has

proc mixJNode(x: var Storage, n: JsonNodeId, kind: JNodeKind) =
  #mixBody JNode
  x.jnodes[n] = JNodeImpl(kind: kind)

proc mixJNull(x: var Storage, n: JsonNodeId) =
  mixBody JNull
  mixJNode(x, n, JNull)

proc mixJBool(x: var Storage, n: JsonNodeId, b: bool) =
  mixBody JBool
  x.jbools[n] = JBoolImpl(bval: b)
  mixJNode(x, n, JBool)

proc mixJInt(x: var Storage, n: JsonNodeId, i: BiggestInt) =
  mixBody JInt
  x.jints[n] = JIntImpl(num: i)
  mixJNode(x, n, JInt)

proc mixJFloat(x: var Storage, n: JsonNodeId, f: float) =
  mixBody JFloat
  x.jfloats[n] = JFloatImpl(fnum: f)
  mixJNode(x, n, JFloat)

proc mixJString(x: var Storage, n: JsonNodeId, s: sink string) =
  mixBody JString
  x.jstrings[n] = JStringImpl(str: s)
  mixJNode(x, n, JString)

proc mixJRawNumber(x: var Storage, n: JsonNodeId, s: sink string) =
  mixBody {JString, JRawNumber}
  x.jstrings[n] = JStringImpl(str: s)
  mixJNode(x, n, JString)

proc raiseDuplicateKeyError(key: string) {.noinline, noreturn, used.} =
  raise newException(KeyError, "key already exists in object: " & key)

proc mixJKey(x: var Storage, n: JsonNodeId, key: sink string) =
  mixBody {JKey, JString}
  mixJNode(x, n, JKey)
  x.jnodes[n].hcode = hash(key)
  x.jstrings[n] = JStringImpl(str: key)

proc mixJObject(x: var Storage, n: JsonNodeId) =
  mixBody JObject
  mixJNode(x, n, JObject)

proc mixJArray(x: var Storage, n: JsonNodeId) =
  mixBody JArray
  mixJNode(x, n, JArray)

func isNil*(x: JsonNode): bool {.inline.} = x.k == nil or isEmpty(x.k[], x.id)

proc kind*(x: JsonNode): JNodeKind {.inline.} =
  assert not x.isNil or JNode in x.k.signatures[x.id]
  result = x.k.jnodes[x.id].kind

proc len*(x: JsonNode): int =
  case x.kind
  of JArray:
    result = x.k.jnodes[x.id].indexes.len
  of JObject:
    result = x.k.jnodes[x.id].counter
  else: discard

iterator items*(x: JsonNode): JsonNode =
  ## Iterator for the items of `x`. `x` has to be a JArray.
  assert x.kind == JArray
  template jarray: untyped = x.k.jnodes[x.id]

  for child in jarray.indexes.items:
    yield JsonNode(id: child, k: x.k)

iterator pairs*(x: JsonNode): (lent string, JsonNode) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert x.kind == JObject
  template jobject: untyped = x.k.jnodes[x.id]

  for child in jobject.keys.items:
    if isFilled(x.k[], child):
      template jstring: untyped = x.k.jstrings[child]
      template jkey: untyped = x.k.jnodes[child]

      assert x.k.signatures[child] * {JKey, JString} == {JKey, JString}
      yield (jstring.str, JsonNode(id: jkey.son, k: x.k))

proc getStr*(x: JsonNode, default: string = ""): string =
  ## Retrieves the string value of a `JString`.
  ##
  ## Returns `default` if `x` is not a `JString`.
  if x.isNil or x.kind == JString: result = x.k.jstrings[x.id].str
  else: result = default

proc getInt*(x: JsonNode, default: int = 0): int =
  ## Retrieves the int value of a `JInt`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if x.isNil or x.kind == JInt: result = int(x.k.jints[x.id].num)
  else: result = default

proc getBiggestInt*(x: JsonNode, default: BiggestInt = 0): BiggestInt =
  ## Retrieves the BiggestInt value of a `JInt`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if x.isNil or x.kind == JInt: result = x.k.jints[x.id].num
  else: result = default

proc getFloat*(x: JsonNode, default: float = 0.0): float =
  ## Retrieves the float value of a `JFloat`.
  ##
  ## Returns `default` if `x` is not a `JFloat` or `JInt`, or if `x` is nil.
  if x.isNil: return default
  case x.kind
  of JFloat:
    result = x.k.jfloats[x.id].fnum
  of JInt:
    result = float(x.k.jints[x.id].num)
  else:
    result = default

proc getBool*(x: JsonNode, default: bool = false): bool =
  ## Retrieves the bool value of a `JBool`.
  ##
  ## Returns `default` if `n` is not a `JBool`, or if `n` is nil.
  if x.isNil or x.kind == JBool: result = x.k.jbools[x.id].bval
  else: result = default

proc contains*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `n`.
  assert x.kind == JObject
  result = get(x.k[], x.id, key) != invalidId

proc hasKey*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `x`.
  result = x.contains(key)

proc raiseKeyError(key: string) {.noinline, noreturn.} =
  raise newException(KeyError, "key not in object: " & key)

proc deleteImpl(x: var Storage, n: JsonNodeId) =
  template jnode: untyped = x.jnodes[n]
  template jstring: untyped = x.jstrings[n]

  case jnode.kind
  of JObject:
    for j in jnode.keys:
      if isFilled(x, j): deleteImpl(x, j)
    reset(jnode.keys)
  of JArray:
    for j in jnode.indexes: deleteImpl(x, j)
    reset(jnode.indexes)
  of JKey:
    deleteImpl(x, jnode.son)
    reset(jstring.str)
  of JString:
    reset(jstring.str)
  else: discard
  x.signatures.del(n)

proc delete*(x: JsonNode, key: string) =
  ## Deletes `x[key]`.
  assert x.kind == JObject
  let id = exclAndGet(x.k[], x.id, key)
  if id != invalidId: deleteImpl(x.k[], id)
  else: raiseKeyError(key)

proc `[]`*(x: JsonNode, key: string): JsonNode {.inline.} =
  ## Gets a field from a `JObject`, which must not be nil.
  ## If the value at `key` does not exist, raises KeyError.
  assert x.kind == JObject
  template jkey: untyped = x.k.jnodes[id]
  let id = get(x.k[], x.id, key)
  if id != invalidId: result = JsonNode(id: jkey.son, k: x.k)
  else: raiseKeyError(key)

proc `[]`*(x: JsonNode, index: int): JsonNode {.inline.} =
  ## Gets the node at `index` in an Array. Result is undefined if `index`
  ## is out of bounds, but as long as array bound checks are enabled it will
  ## result in an exception.
  assert x.kind == JArray
  template jarray: untyped = x.k.jnodes[x.id]
  let id = jarray.indexes[index]
  result = JsonNode(id: id, k: x.k)

proc `{}`*(x: JsonNode, keys: varargs[string]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## keys do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an object.
  template jkey: untyped = x.k.jnodes[resultId]
  var resultId = x.id
  for kk in keys:
    if x.k.isNil or isEmpty(x.k[], resultId) or
        JObject notin x.k.signatures[resultId]:
      return JsonNode(id: invalidId, k: nil)
    resultId = get(x.k[], resultId, kk)
    if resultId == invalidId: return JsonNode(id: invalidId, k: nil)
    resultId = jkey.son
  result = JsonNode(id: resultId, k: x.k)

proc `{}`*(x: JsonNode, indexes: varargs[int]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## indexes do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an array.
  template jarray: untyped = x.k.jnodes[resultId]
  var resultId = x.id
  for j in indexes:
    if x.k.isNil or isEmpty(x.k[], resultId) or
        JArray notin x.k.signatures[resultId] or j >= jarray.indexes.len:
      return JsonNode(id: invalidId, k: nil)
    resultId = jarray.indexes[j]
  result = JsonNode(id: resultId, k: x.k)

proc skipJson(p: var JsonParser) =
  case p.tok
  of tkString, tkInt, tkFloat, tkTrue, tkFalse, tkNull:
    discard getTok(p)
  of tkCurlyLe:
    discard getTok(p)
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raiseParseErr(p, "string literal as key")
      discard getTok(p)
      eat(p, tkColon)
      skipJson(p)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkCurlyRi)
  of tkBracketLe:
    discard getTok(p)
    while p.tok != tkBracketRi:
      skipJson(p)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkBracketRi)
  of tkError, tkCurlyRi, tkBracketRi, tkColon, tkComma, tkEof:
    raiseParseErr(p, "{")

proc parseJson(x: var Storage; p: var JsonParser; rawIntegers, rawFloats: bool): JsonNodeId =
  case p.tok
  of tkString:
    result = getJsonNodeId(x)
    mixJString(x, result, p.a)
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
    discard getTok(p)
  of tkTrue:
    result = getJsonNodeId(x)
    mixJBool(x, result, true)
    discard getTok(p)
  of tkFalse:
    result = getJsonNodeId(x)
    mixJBool(x, result, false)
    discard getTok(p)
  of tkNull:
    result = getJsonNodeId(x)
    mixJNull(x, result)
    discard getTok(p)
  of tkCurlyLe:
    result = getJsonNodeId(x)
    mixJObject(x, result)
    discard getTok(p)
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raiseParseErr(p, "string literal as key")
      let key = getJsonNodeId(x)
      mixJKey(x, key, p.a)
      template jkey: untyped = x.jnodes[key]
      if getOrIncl(x, result, key, p.a) == key:
        discard getTok(p)
        eat(p, tkColon)
        let son = parseJson(x, p, rawIntegers, rawFloats)
        jkey.son = son
      else:
        when defined(jsonStrict):
          raiseDuplicateKeyError(p.a)
        else:
          discard getTok(p)
          eat(p, tkColon)
          skipJson(p)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkCurlyRi)
  of tkBracketLe:
    result = getJsonNodeId(x)
    template jarray: untyped = x.jnodes[result]
    mixJArray(x, result)
    discard getTok(p)
    while p.tok != tkBracketRi:
      let elem = parseJson(x, p, rawIntegers, rawFloats)
      jarray.indexes.add elem
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
    new(result.k)
    result.k[] = Storage(
        signatures: newSlotTableOfCap[set[JNodeKind]](),
        jbools: newSecTable[JBoolImpl](),
        jints: newSecTable[JIntImpl](),
        jfloats: newSecTable[JFloatImpl](),
        jstrings: newSecTable[JStringImpl](),
        jnodes: newSecTable[JNodeImpl]()
      )
    result.id = parseJson(result.k[], p, rawIntegers, rawFloats)
    eat(p, tkEof)
  finally:
    close(p)

proc parseJson*(buffer: string; rawIntegers = false, rawFloats = false): JsonNode =
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
  block: # basic structure
    let testJson = parseJson"""{"a": [1, 2, {"key": [4, 5]}, 4]}"""
    for (key, node1) in pairs(testJson):
      assert key == "a"
      assert node1.kind == JArray
      let vals = @[1, 2, 0, 4]
      var i = 0
      for node2 in items(node1):
        assert node2.getInt() == vals[i]
        inc i
  block: # key iterator order
    let testJson = parseJson"""{"name": "Isaac", "books": ["Robot Dreams"],
        "details": {"age": 35, "pi": 3.1415}}"""
    var i = 0
    let keys = @[("name", JString), ("books", JArray), ("details", JObject)]
    for (key, node) in pairs(testJson):
      assert keys[i][1] == node.kind
      assert keys[i][0] == key
      inc i
    assert testJson{"details", "age"}.getInt() == 35
  block: # duplicate keys
    let testJson = parseJson"""{"a": "Isaac", "b": {"stuff": 42}, "b": {"stuff": 69}}"""
    assert testJson.len == 2
    assert testJson{"b", "stuff"}.getInt() == 42
  block: # collisions
    let testJson = parseJson"""{"geh": "hello", "aeh": 2, "ahg": "world", "abd": 4, "ceh": 5, "fda": 6}"""
    assert testJson["geh"].getStr() == "hello"
    assert testJson.len == 6
    delete testJson, "geh"
    delete testJson, "ceh"
    assert testJson["abd"].getInt() == 4
    assert testJson["fda"].getInt() == 6
    assert testJson.len == 4
    delete testJson, "ahg"
    assert testJson["fda"].getInt() == 6
