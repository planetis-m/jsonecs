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
    head, tail: JsonNode
    prev, next: JsonNode
    parent: JsonNode

  JsonTree* = object
    signatures: SlotTable[set[JsonNodeKind]]
    # Atoms
    jbools: seq[JBoolImpl]
    jints: seq[JIntImpl]
    jfloats: seq[JFloatImpl]
    jstrings: seq[JStringImpl]
    # Mappings
    hierarchies: seq[Hierarchy]

proc `=copy`*(dest: var JsonTree; source: JsonTree) {.error.}

proc createJsonNode(x: var JsonTree): JsonNode =
  result = x.signatures.incl({})

iterator queryAll(x: JsonTree, parent: JsonNode, query: set[JsonNodeKind]): JsonNode =
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
proc append(x: var JsonTree, parentId, n: JsonNode) =
  template hierarchy: untyped = x.hierarchies[n.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template tailSibling: untyped = x.hierarchies[tailSiblingId.idx]

  hierarchy.next = invalidId
  hierarchy.prev = parent.tail
  if tailSiblingId ?= parent.tail:
    assert tailSibling.next == invalidId
    tailSibling.next = n
  parent.tail = n
  if parent.head == invalidId: parent.head = n

proc removeNode(x: var JsonTree, n: JsonNode) =
  template hierarchy: untyped = x.hierarchies[n.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template nextSibling: untyped = x.hierarchies[nextSiblingId.idx]
  template prevSibling: untyped = x.hierarchies[prevSiblingId.idx]

  if parentId ?= hierarchy.parent:
    if n == parent.tail: parent.tail = hierarchy.prev
    if n == parent.head: parent.head = hierarchy.next
  if nextSiblingId ?= hierarchy.next: nextSibling.prev = hierarchy.prev
  if prevSiblingId ?= hierarchy.prev: prevSibling.next = hierarchy.next

proc delete*(x: var JsonTree, n: JsonNode) =
  ## Deletes ``x[key]``.
  var toDelete: seq[JsonNode]
  for entity in queryAll(x, n, {HierarchyPriv}):
    removeNode(x, entity)
    toDelete.add(entity)
  for entity in toDelete.items:
    x.signatures.del(entity)

template mixBody(has) =
  x.signatures[n].incl has

proc reserve[T](x: var seq[T]; needed: int) {.inline.} =
  if needed > x.len: setLen(x, needed)

proc mixHierarchy(x: var JsonTree, n: JsonNode, parent = invalidId) =
  mixBody HierarchyPriv
  reserve(x.hierarchies, n.idx + 1)
  x.hierarchies[n.idx] = Hierarchy(
    head: invalidId, tail: invalidId, prev: invalidId, next: invalidId,
    parent: parent)
  if parent != invalidId: append(x, parent, n)

proc mixJNull(x: var JsonTree, n: JsonNode) =
  mixBody JNull

proc mixJBool(x: var JsonTree, n: JsonNode, b: bool) =
  mixBody JBool
  reserve(x.jbools, n.idx + 1)
  x.jbools[n.idx] = JBoolImpl(bval: b)

proc mixJInt(x: var JsonTree, n: JsonNode, i: BiggestInt) =
  mixBody JInt
  reserve(x.jints, n.idx + 1)
  x.jints[n.idx] = JIntImpl(num: i)

proc mixJFloat(x: var JsonTree, n: JsonNode, f: float) =
  mixBody JFloat
  reserve(x.jfloats, n.idx + 1)
  x.jfloats[n.idx] = JFloatImpl(fnum: f)

proc mixJString(x: var JsonTree, n: JsonNode, s: sink string) =
  mixBody JString
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s)

proc mixJRawNumber(x: var JsonTree, n: JsonNode, s: sink string) =
  mixBody JString
  mixBody JRawNumber
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s, isUnquoted: true)

proc mixJKey(x: var JsonTree, n: JsonNode, s: sink string, parent = invalidId) =
  mixBody JKey
  mixBody JString
  reserve(x.jstrings, n.idx + 1)
  x.jstrings[n.idx] = JStringImpl(str: s)
  mixHierarchy(x, n, parent)

proc mixJObject(x: var JsonTree, n: JsonNode, parent = invalidId) =
  mixBody JObject
  mixHierarchy(x, n, parent)

proc mixJArray(x: var JsonTree, n: JsonNode, parent = invalidId) =
  mixBody JArray
  mixHierarchy(x, n, parent)

proc kind*(x: JsonTree, n: JsonNode): JsonNodeKind =
  let sign = x.signatures[n]
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

iterator items*(x: JsonTree, n: JsonNode): JsonNode =
  ## Iterator for the items of `x`. `x` has to be a JArray.
  assert JArray in x.signatures[n]
  template hierarchy: untyped = x.hierarchies[n.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.hierarchies[childId.idx]

    yield childId
    childId = childHierarchy.next

iterator pairs*(x: JsonTree, n: JsonNode): (string, JsonNode) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert JObject in x.signatures[n]
  template hierarchy: untyped = x.hierarchies[n.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.hierarchies[childId.idx]

    assert JKey in x.signatures[childId]
    yield (x.jstrings[childId.idx].str, childHierarchy.head)
    childId = childHierarchy.next

proc getStr*(x: JsonTree, n: JsonNode, default: string = ""): string =
  ## Retrieves the string value of a `JString JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JString``.
  if JString in x.signatures[n]: result = x.jstrings[n.idx].str
  else: result = default

proc getInt*(x: JsonTree, n: JsonNode, default: int = 0): int =
  ## Retrieves the int value of a `JInt JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JInt``, or if ``n`` is nil.
  if JInt in x.signatures[n]: result = int(x.jints[n.idx].num)
  else: result = default

proc getBiggestInt*(x: JsonTree, n: JsonNode, default: BiggestInt = 0): BiggestInt =
  ## Retrieves the BiggestInt value of a `JInt JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JInt``, or if ``n`` is nil.
  if JInt in x.signatures[n]: result = x.jints[n.idx].num
  else: result = default

proc getFloat*(x: JsonTree, n: JsonNode, default: float = 0.0): float =
  ## Retrieves the float value of a `JFloat JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JFloat`` or ``JInt``, or if ``n`` is nil.
  let sign = x.signatures[n]
  if JFloat in sign:
    result = x.jfloats[n.idx].fnum
  elif JInt in sign:
    result = float(x.jints[n.idx].num)
  else:
    result = default

proc getBool*(x: JsonTree, n: JsonNode, default: bool = false): bool =
  ## Retrieves the bool value of a `JBool JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JBool``, or if ``n`` is nil.
  if JBool in x.signatures[n]: result = x.jbools[n.idx].bval
  else: result = default

proc parseJson(x: var JsonTree; p: var JsonParser; rawIntegers, rawFloats: bool;
      parent: JsonNode): JsonNode =
  case p.tok
  of tkString:
    result = createJsonNode(x)
    mixJString(x, result, p.a)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkInt:
    result = createJsonNode(x)
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
    result = createJsonNode(x)
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
    result = createJsonNode(x)
    mixJBool(x, result, true)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkFalse:
    result = createJsonNode(x)
    mixJBool(x, result, false)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkNull:
    result = createJsonNode(x)
    mixJNull(x, result)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkCurlyLe:
    result = createJsonNode(x)
    mixJObject(x, result, parent)
    discard getTok(p)
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raiseParseErr(p, "string literal as key")
      let key = createJsonNode(x)
      mixJKey(x, key, p.a, result)
      discard getTok(p)
      eat(p, tkColon)
      discard parseJson(x, p, rawIntegers, rawFloats, key)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkCurlyRi)
  of tkBracketLe:
    result = createJsonNode(x)
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
    rawIntegers = false, rawFloats = false): (JsonNode, JsonTree) =
  ## Parses from a stream `s` into a `JsonTree`. `filename` is only needed
  ## for nice error messages.
  ## If `s` contains extra data, it will raise `JsonParsingError`.
  var p: JsonParser
  open(p, s, filename)
  try:
    discard getTok(p)
    result[1] = JsonTree()
    result[0] = parseJson(result[1], p, rawIntegers, rawFloats, invalidId)
    eat(p, tkEof)
  finally:
    close(p)

proc parseJson*(buffer: string;
    rawIntegers = false, rawFloats = false): (JsonNode, JsonTree) =
  ## Parses JSON from `buffer`.
  ## If `buffer` contains extra data, it will raise `JsonParsingError`.
  parseJson(newStringStream(buffer), "input", rawIntegers, rawFloats)

proc parseFile*(filename: string): (JsonNode, JsonTree) =
  ## Parses `file` into a `JsonTree`.
  ## If `file` contains extra data, it will raise `JsonParsingError`.
  var stream = newFileStream(filename, fmRead)
  if stream == nil:
    raise newException(IOError, "cannot read from file: " & filename)
  result = parseJson(stream, filename)

when isMainModule:
  block:
    let (rootNode, testJson) = parseJson"""{ "a": [1, 2, {"key": [4, 5]}, 4]}"""
    echo testJson.signatures.len
    for (key, node1) in pairs(testJson, rootNode):
      echo key, ": ", testJson.kind(node1)
      for node2 in items(testJson, node1):
        echo testJson.getInt(node2)
  block:
    let (rootNode, testJson) = parseJson"""{"name": "Isaac", "books": ["Robot Dreams"],
        "details": {"age": 35, "pi": 3.1415}}"""
    echo testJson.signatures.len
    for (key, node1) in pairs(testJson, rootNode):
      echo key, ": ", testJson.kind(node1)
