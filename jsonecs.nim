import
  jsonecs / [entities, slottables, heaparrays],
  std / [parsejson, streams, strutils]
export entities

type
  HasComponent* = enum
    HasJNull
    HasJBool
    HasJInt
    HasJFloat
    HasJString
    HasJObject
    HasJArray
    HasJKey
    HasJRawNumber
    HasHierarchy

  JBool* = object
    bval*: bool

  JInt* = object
    num*: BiggestInt

  JFloat* = object
    fnum*: float

  JString* = object
    str*: string
    isUnquoted: bool

  Hierarchy* = object
    head*: Entity
    prev*, next*: Entity
    parent*: Entity

  JsonTree* = object
    signatures*: SlotTable[set[HasComponent]]
    # Atoms
    jbools*: Array[JBool]
    jints*: Array[JInt]
    jfloats*: Array[JFloat]
    jstrings*: Array[JString]
    # Mappings
    hierarchies*: Array[Hierarchy]
    # Root JsonNode
    root*: Entity

proc createEntity*(x: var JsonTree): Entity =
  result = x.signatures.incl({})

iterator queryAll(x: JsonTree, parent: Entity, query: set[HasComponent]): Entity =
  template hierarchy: untyped = x.hierarchies[e.idx]

  var frontier = @[parent]
  while frontier.len > 0:
    let e = frontier.pop()
    if x.signatures[e] * query == query:
      yield e
    var childId = hierarchy.head
    while childId != invalidId:
      template childHierarchy: untyped = x.hierarchies[childId.idx]

      frontier.add(childId)
      childId = childHierarchy.next

template `?=`(name, value): bool = (let name = value; name != invalidId)
proc prepend(x: var JsonTree, parentId, e: Entity) =
  template hierarchy: untyped = x.hierarchies[e.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template headSibling: untyped = x.hierarchies[headSiblingId.idx]

  hierarchy.prev = invalidId
  hierarchy.next = parent.head
  if headSiblingId ?= parent.head:
    assert headSibling.prev == invalidId
    headSibling.prev = e
  parent.head = e

template mixBody(has) =
  x.signatures[e].incl has

proc mixHierarchy(x: var JsonTree, e: Entity, parent = invalidId) =
  mixBody HasHierarchy
  x.hierarchies[e.idx] = Hierarchy(head: invalidId, prev: invalidId,
      next: invalidId, parent: parent)
  if parent != invalidId: prepend(x, parent, e)

proc mixJNull(x: var JsonTree, e: Entity) =
  mixBody HasJNull

proc mixJBool(x: var JsonTree, e: Entity, b: bool) =
  mixBody HasJBool
  x.jbools[e.idx] = JBool(bval: b)

proc mixJInt(x: var JsonTree, e: Entity, n: BiggestInt) =
  mixBody HasJInt
  x.jints[e.idx] = JInt(num: n)

proc mixJFloat(x: var JsonTree, e: Entity, n: float) =
  mixBody HasJFloat
  x.jfloats[e.idx] = JFloat(fnum: n)

proc mixJString(x: var JsonTree, e: Entity, s: sink string) =
  mixBody HasJString
  x.jstrings[e.idx] = JString(str: s)

proc mixJRawNumber(x: var JsonTree, e: Entity, s: sink string) =
  mixBody HasJRawNumber
  mixBody HasJString
  x.jstrings[e.idx] = JString(str: s, isUnquoted: true)

proc mixJKey(x: var JsonTree, e: Entity, s: sink string, parent = invalidId) =
  mixBody HasJKey
  mixBody HasJString
  x.jstrings[e.idx] = JString(str: s)
  mixHierarchy(x, e, parent)

proc mixJObject(x: var JsonTree, e: Entity, parent = invalidId) =
  mixBody HasJObject
  mixHierarchy(x, e, parent)

proc mixJArray(x: var JsonTree, e: Entity, parent = invalidId) =
  mixBody HasJArray
  mixHierarchy(x, e, parent)

proc parseJson(x: var JsonTree; p: var JsonParser; rawIntegers, rawFloats: bool;
      parent: Entity): Entity =
  case p.tok
  of tkString:
    result = createEntity(x)
    mixJString(x, result, p.a)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkInt:
    result = createEntity(x)
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
    result = createEntity(x)
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
    result = createEntity(x)
    mixJBool(x, result, true)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkFalse:
    result = createEntity(x)
    mixJBool(x, result, false)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkNull:
    result = createEntity(x)
    mixJNull(x, result)
    mixHierarchy(x, result, parent)
    discard getTok(p)
  of tkCurlyLe:
    result = createEntity(x)
    mixJObject(x, result, parent)
    discard getTok(p)
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raiseParseErr(p, "string literal as key")
      let key = createEntity(x)
      mixJKey(x, key, p.a, result)
      discard getTok(p)
      eat(p, tkColon)
      discard parseJson(x, p, rawIntegers, rawFloats, key)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkCurlyRi)
  of tkBracketLe:
    result = createEntity(x)
    mixJArray(x, result, parent)
    discard getTok(p)
    while p.tok != tkBracketRi:
      discard parseJson(x, p, rawIntegers, rawFloats, result)
      if p.tok != tkComma: break
      discard getTok(p)
    eat(p, tkBracketRi)
  of tkError, tkCurlyRi, tkBracketRi, tkColon, tkComma, tkEof:
    raiseParseErr(p, "{")

proc parseJson*(s: Stream, filename: string = ""; rawIntegers = false, rawFloats = false): JsonTree =
  var p: JsonParser
  open(p, s, filename)
  try:
    discard getTok(p)
    result = JsonTree(
      signatures: initSlotTableOfCap[set[HasComponent]](maxEntities),
      jbools: initArray[JBool](),
      jints: initArray[JInt](),
      jfloats: initArray[JFloat](),
      jstrings: initArray[JString](),
      hierarchies: initArray[Hierarchy]()
    )
    let root = parseJson(result, p, rawIntegers, rawFloats, invalidId)
    result.root = root
    eat(p, tkEof)
  finally:
    close(p)

proc parseJson*(buffer: string; rawIntegers = false, rawFloats = false): JsonTree =
  result = parseJson(newStringStream(buffer), "input", rawIntegers, rawFloats)

when isMainModule:
  let testJson = parseJson"""{ "a": [1, 2, {"key": [4, 5]}, 4]}"""
  echo testJson.signatures.len
  for ent in queryAll(testJson, testJson.root, {HasHierarchy}):
    echo testJson.signatures[ent]
    if HasJString in testJson.signatures[ent]:
      echo testJson.jstrings[ent.idx].str
