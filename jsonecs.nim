import
  jsonecs / [entities, slottables, heaparrays],
  std / [parsejson, streams, strutils]
export entities

type
  HasComponent = enum
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

  JBool = object
    bval: bool

  JInt = object
    num: BiggestInt

  JFloat = object
    fnum: float

  JString = object
    str: string
    isUnquoted: bool

  Hierarchy = object
    head: Entity
    prev, next: Entity
    parent: Entity

  JsonTree* = object
    signatures: SlotTable[set[HasComponent]]
    # Atoms
    jbools: Array[JBool]
    jints: Array[JInt]
    jfloats: Array[JFloat]
    jstrings: Array[JString]
    # Mappings
    hierarchies: Array[Hierarchy]

proc createEntity(x: var JsonTree): Entity =
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

proc removeNode(x: var JsonTree, entity: Entity) =
  template hierarchy: untyped = x.hierarchies[entity.idx]
  template parent: untyped = x.hierarchies[parentId.idx]
  template nextSibling: untyped = x.hierarchies[nextSiblingId.idx]
  template prevSibling: untyped = x.hierarchies[prevSiblingId.idx]

  if parentId ?= hierarchy.parent:
    if entity == parent.head: parent.head = hierarchy.next
  if nextSiblingId ?= hierarchy.next: nextSibling.prev = hierarchy.prev
  if prevSiblingId ?= hierarchy.prev: prevSibling.next = hierarchy.next

proc delete*(x: var JsonTree, e: Entity) =
  ## Deletes ``x[key]``.
  var toDelete: seq[Entity]
  for entity in queryAll(x, e, {HasHierarchy}):
    removeNode(x, entity)
    toDelete.add(entity)
  for entity in toDelete.items:
    x.signatures.del(entity)

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

iterator items*(x: JsonTree, e: Entity): Entity =
  ## Iterator for the items of `x`. `x` has to be a JArray.
  assert HasJArray in x.signatures[e]
  template hierarchy: untyped = x.hierarchies[e.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.hierarchies[childId.idx]

    yield childId
    childId = childHierarchy.next

iterator pairs*(x: JsonTree, e: Entity): (string, Entity) =
  ## Iterator for the pairs of `x`. `x` has to be a JObject.
  assert HasJObject in x.signatures[e]
  template hierarchy: untyped = x.hierarchies[e.idx]

  var childId = hierarchy.head
  while childId != invalidId:
    template childHierarchy: untyped = x.hierarchies[childId.idx]

    assert HasJKey in x.signatures[childId]
    yield (x.jstrings[childId.idx].str, childHierarchy.head)
    childId = childHierarchy.next

proc getStr*(n: JsonTree, e: Entity, default: string = ""): string =
  ## Retrieves the string value of a `JString JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JString``.
  if HasJString in n.signatures[e]: result = n.jstrings[e.idx].str
  else: result = default

proc getInt*(n: JsonTree, e: Entity, default: int = 0): int =
  ## Retrieves the int value of a `JInt JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JInt``, or if ``n`` is nil.
  if HasJInt in n.signatures[e]: result = int(n.jints[e.idx].num)
  else: result = default

proc getBiggestInt*(n: JsonTree, e: Entity, default: BiggestInt = 0): BiggestInt =
  ## Retrieves the BiggestInt value of a `JInt JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JInt``, or if ``n`` is nil.
  if HasJInt in n.signatures[e]: result = n.jints[e.idx].num
  else: result = default

proc getFloat*(n: JsonTree, e: Entity, default: float = 0.0): float =
  ## Retrieves the float value of a `JFloat JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JFloat`` or ``JInt``, or if ``n`` is nil.
  let sign = n.signatures[e]
  if HasJFloat in sign:
    result = n.jfloats[e.idx].fnum
  elif HasJInt in sign:
    result = float(n.jints[e.idx].num)
  else:
    result = default

proc getBool*(n: JsonTree, e: Entity, default: bool = false): bool =
  ## Retrieves the bool value of a `JBool JsonTree`.
  ##
  ## Returns ``default`` if ``n`` is not a ``JBool``, or if ``n`` is nil.
  if HasJBool in n.signatures[e]: result = n.jbools[e.idx].bval
  else: result = default

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

proc parseJson*(s: Stream, filename: string = "";
    rawIntegers = false, rawFloats = false): (Entity, JsonTree) =
  ## Parses from a stream `s` into a `JsonTree`. `filename` is only needed
  ## for nice error messages.
  ## If `s` contains extra data, it will raise `JsonParsingError`.
  var p: JsonParser
  open(p, s, filename)
  try:
    discard getTok(p)
    result[1] = JsonTree(
      signatures: initSlotTableOfCap[set[HasComponent]](maxEntities),
      jbools: initArray[JBool](),
      jints: initArray[JInt](),
      jfloats: initArray[JFloat](),
      jstrings: initArray[JString](),
      hierarchies: initArray[Hierarchy]()
    )
    result[0] = parseJson(result[1], p, rawIntegers, rawFloats, invalidId)
    eat(p, tkEof)
  finally:
    close(p)

proc parseJson*(buffer: string;
    rawIntegers = false, rawFloats = false): (Entity, JsonTree) =
  ## Parses JSON from `buffer`.
  ## If `buffer` contains extra data, it will raise `JsonParsingError`.
  parseJson(newStringStream(buffer), "input", rawIntegers, rawFloats)

proc parseFile*(filename: string): (Entity, JsonTree) =
  ## Parses `file` into a `JsonTree`.
  ## If `file` contains extra data, it will raise `JsonParsingError`.
  var stream = newFileStream(filename, fmRead)
  if stream == nil:
    raise newException(IOError, "cannot read from file: " & filename)
  result = parseJson(stream, filename)

when isMainModule:
  let (root, testJson) = parseJson"""{ "a": [1, 2, {"key": [4, 5]}, 4]}"""
  echo testJson.signatures.len
  for (key, ent1) in pairs(testJson, root):
    echo key, ":"
    for ent2 in items(testJson, ent1):
      echo testJson.getInt(ent2)
