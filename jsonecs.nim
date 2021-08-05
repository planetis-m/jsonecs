import
  jsonecs / [jsonnodes, slottables],
  std / [parsejson, streams, strutils]
export jsonnodes

type
  JsonNodeKind* = enum
    JNull
    JBool
    JInt
    JFloat
    JString
    JObject
    JArray
    JKey
    JRawNumber
    HierarchyPriv

  JBoolImpl = object
    bval: bool

  JIntImpl = object
    num: BiggestInt

  JFloatImpl = object
    fnum: float

  JStringImpl = object
    str: string
    isUnquoted: bool

  Hierarchy = object
    head, tail: JsonNodeId
    next: JsonNodeId
    parent: JsonNodeId

  JsonTree* = ref object
    signatures: SlotTable[set[JsonNodeKind]]
    # Atoms
    jbools: seq[JBoolImpl]
    jints: seq[JIntImpl]
    jfloats: seq[JFloatImpl]
    jstrings: seq[JStringImpl]
    # Mappings
    hierarchies: seq[Hierarchy]

  JsonNode* = object
    id: JsonNodeId
    k: JsonTree

proc getJsonNodeId(x: JsonTree): JsonNodeId =
  result = x.signatures.incl({})

iterator queryAll(x: JsonTree, parent: JsonNodeId, query: set[JsonNodeKind]): JsonNodeId =
  template hierarchy: untyped = x.hierarchies[n.idx]

  var frontier = @[parent]
  while frontier.len > 0:
    let n = frontier.pop()
    if x.signatures[n] * query == query:
      yield n
    var childId = hierarchy.head
    while childId != invalidId:
      template childHierarchy: untyped = x.hierarchies[childId.idx]

      frontier.add(childId)
      childId = childHierarchy.next

template `?=`(name, value): bool = (let name = value; name != invalidId)
proc append(x: JsonTree, parentId, n: JsonNodeId) =
  template hierarchy: untyped = x.hierarchies[n.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template tailSibling: untyped = x.hierarchies[tailSiblingId.idx]

  hierarchy.next = invalidId
  if tailSiblingId ?= parent.tail:
    assert tailSibling.next == invalidId
    tailSibling.next = n
  parent.tail = n
  if parent.head == invalidId: parent.head = n

proc removeNode(x: JsonTree, n: JsonNodeId) =
  template hierarchy: untyped = x.hierarchies[n.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template nextSibling: untyped = x.hierarchies[nextSiblingId.idx]
  template tailSibling: untyped = x.hierarchies[tailSiblingId.idx]

  if parentId ?= hierarchy.parent:
    if n == parent.head:
      parent.head = hierarchy.next
      if tailSiblingId ?= parent.tail:
        if tailSibling.next == n:
          tailSibling.next = parent.head

proc delete*(x: JsonTree, n: JsonNodeId) =
  ## Deletes `x[key]`.
  var toDelete: seq[JsonNodeId]
  for entity in queryAll(x, n, {HierarchyPriv}):
    removeNode(x, entity)
    toDelete.add(entity)
  for entity in toDelete.items:
    x.signatures.del(entity)

template mixBody(has) =
  x.signatures[n].incl has

proc reserve[T](x: var seq[T]; needed: int) {.inline.} =
  if needed > x.len: setLen(x, needed)

proc mixHierarchy(x: JsonTree, n: JsonNodeId, parent = invalidId) =
  mixBody HierarchyPriv
  reserve(x.hierarchies, n.idx + 1)
  x.hierarchies[n.idx] = Hierarchy(
    head: invalidId, tail: invalidId, next: invalidId,
    parent: parent)
  if parent != invalidId: append(x, parent, n)

proc mixJNull(x: JsonTree, n: JsonNodeId) =
  mixBody JNull

proc mixJBool(x: JsonTree, n: JsonNodeId, b: bool) =
  mixBody JBool
  reserve(x.jbools, n.idx + 1)
  x.jbools[n.idx] = JBoolImpl(bval: b)

proc mixJInt(x: JsonTree, n: JsonNodeId, i: BiggestInt) =
  mixBody JInt
  reserve(x.jints, n.idx + 1)
  x.jints[n.idx] = JIntImpl(num: i)

proc mixJFloat(x: JsonTree, n: JsonNodeId, f: float) =
  mixBody JFloat
  reserve(x.jfloats, n.idx + 1)
  x.jfloats[n.idx] = JFloatImpl(fnum: f)

proc mixJString(x: JsonTree, n: JsonNodeId, s: sink string) =
  mixBody JString
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s)

proc mixJRawNumber(x: JsonTree, n: JsonNodeId, s: sink string) =
  mixBody JString
  mixBody JRawNumber
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s, isUnquoted: true)

proc mixJKey(x: JsonTree, n: JsonNodeId, s: sink string, parent = invalidId) =
  mixBody JKey
  mixBody JString
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s)
  mixHierarchy(x, n, parent)

proc mixJObject(x: JsonTree, n: JsonNodeId, parent = invalidId) =
  mixBody JObject
  mixHierarchy(x, n, parent)

proc mixJArray(x: JsonTree, n: JsonNodeId, parent = invalidId) =
  mixBody JArray
  mixHierarchy(x, n, parent)

proc kind*(x: JsonNode): JsonNodeKind =
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
  assert JArray in x.k.signatures[x.id]
  template hierarchy: untyped = x.k.hierarchies[x.id.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.k.hierarchies[childId.idx]

    yield JsonNode(id: childId, k: x.k)
    childId = childHierarchy.next

iterator pairs*(x: JsonNode): (string, JsonNode) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert JObject in x.k.signatures[x.id]
  template hierarchy: untyped = x.k.hierarchies[x.id.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.k.hierarchies[childId.idx]

    assert JKey in x.k.signatures[childId]
    yield (x.k.jstrings[childId.idx].str, JsonNode(id: childHierarchy.head, k: x.k))
    childId = childHierarchy.next

proc getStr*(x: JsonNode, default: string = ""): string =
  ## Retrieves the string value of a `JString JsonTree`.
  ##
  ## Returns `default` if `x` is not a `JString`.
  if JString in x.k.signatures[x.id]: result = x.k.jstrings[x.id.idx].str
  else: result = default

proc getInt*(x: JsonNode, default: int = 0): int =
  ## Retrieves the int value of a `JInt JsonTree`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if JInt in x.k.signatures[x.id]: result = int(x.k.jints[x.id.idx].num)
  else: result = default

proc getBiggestInt*(x: JsonNode, default: BiggestInt = 0): BiggestInt =
  ## Retrieves the BiggestInt value of a `JInt JsonTree`.
  ##
  ## Returns `default` if `x` is not a `JInt`, or if `x` is nil.
  if JInt in x.k.signatures[x.id]: result = x.k.jints[x.id.idx].num
  else: result = default

proc getFloat*(x: JsonNode, default: float = 0.0): float =
  ## Retrieves the float value of a `JFloat JsonTree`.
  ##
  ## Returns `default` if `x` is not a `JFloat` or `JInt`, or if `x` is nil.
  let sign = x.k.signatures[x.id]
  if JFloat in sign:
    result = x.k.jfloats[x.id.idx].fnum
  elif JInt in sign:
    result = float(x.k.jints[x.id.idx].num)
  else:
    result = default

proc getBool*(x: JsonNode, default: bool = false): bool =
  ## Retrieves the bool value of a `JBool JsonTree`.
  ##
  ## Returns `default` if `n` is not a `JBool`, or if `n` is nil.
  if JBool in x.k.signatures[x.id]: result = x.k.jbools[x.id.idx].bval
  else: result = default

proc `[]`*(x: JsonNode, key: string): JsonNode {.inline.} =
  ## Gets a field from a `JObject`, which must not be nil.
  ## If the value at `key` does not exist, raises KeyError.
  for o, y in pairs(x):
    if o == key: return y
  raise newException(KeyError, "key not found in object: " & key)

proc `[]`*(x: JsonNode, index: int): JsonNode {.inline.} =
  ## Gets the node at `index` in an Array. Result is undefined if `index`
  ## is out of bounds, but as long as array bound checks are enabled it will
  ## result in an exception.
  var i = index
  for y in items(x):
    if i == 0: return y
    dec i
  raise newException(IndexDefect, "index out of bounds")

proc contains*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `n`.
  result = false
  for o, y in pairs(x):
    if o == key: return true

proc hasKey*(x: JsonNode, key: string): bool =
  ## Checks if `key` exists in `x`.
  result = x.contains(key)

proc `{}`*(x: JsonNode, keys: varargs[string]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## keys do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an object.
  result = x
  for kk in keys:
    if JObject notin x.k.signatures[result.id]: return JsonNode(id: invalidId)
    block searchLoop:
      for k, v in pairs(result):
        if k == kk:
          result = v
          break searchLoop
      return JsonNode(id: invalidId)

proc `{}`*(x: JsonNode, indexes: varargs[int]): JsonNode =
  ## Traverses the node and gets the given value. If any of the
  ## indexes do not exist, returns `JNull`. Also returns `JNull` if one of the
  ## intermediate data structures is not an array.
  result = x
  for j in indexes:
    if JArray notin x.k.signatures[result.id]: return JsonNode(id: invalidId)
    block searchLoop:
      var i = j
      for y in items(result):
        if i == 0:
          result = y
          break searchLoop
        dec i
      return JsonNode(id: invalidId)

proc parseJson(x: var JsonTree; p: var JsonParser; rawIntegers, rawFloats: bool;
      parent: JsonNodeId): JsonNodeId =
  case p.tok
  of tkString:
    result = getJsonNodeId(x)
    mixJString(x, result, p.a)
    mixHierarchy(x, result, parent)
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
    mixHierarchy(x, result, parent)
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
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkTrue:
    result = getJsonNodeId(x)
    mixJBool(x, result, true)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkFalse:
    result = getJsonNodeId(x)
    mixJBool(x, result, false)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkNull:
    result = getJsonNodeId(x)
    mixJNull(x, result)
    mixHierarchy(x, result, parent)
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
  ## Parses from a stream `s` into a `JsonTree`. `filename` is only needed
  ## for nice error messages.
  ## If `s` contains extra data, it will raise `JsonParsingError`.
  var p: JsonParser
  open(p, s, filename)
  try:
    discard getTok(p)
    result.k = JsonTree()
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
  ## Parses `file` into a `JsonTree`.
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
