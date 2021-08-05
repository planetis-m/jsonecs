import jsonnodes

type
  Entry*[T] = tuple
    n: JsonNode
    value: T

  SlotTable*[T] = object
    freeHead: int
    slots: seq[JsonNode]
    data: seq[Entry[T]]

proc initSlotTableOfCap*[T](capacity: Natural): SlotTable[T] =
  result = SlotTable[T](
    data: newSeqOfCap[Entry[T]](capacity),
    slots: newSeqOfCap[JsonNode](capacity),
    freeHead: 0
  )

proc len*[T](x: SlotTable[T]): int {.inline.} =
  x.data.len

proc contains*[T](x: SlotTable[T], n: JsonNode): bool {.inline.} =
  n.idx < x.slots.len and x.slots[n.idx].version == n.version

proc raiseRangeDefect() {.noinline, noreturn.} =
  raise newException(RangeDefect, "SlotTable number of elements overflow")

proc incl*[T](x: var SlotTable[T], value: T): JsonNode =
  when compileOption("boundChecks"):
    if x.len + 1 == maxJsonNodes:
      raiseRangeDefect()
  let idx = x.freeHead
  if idx < x.slots.len:
    template slot: untyped = x.slots[idx]
    let occupiedVersion = slot.version or 1
    result = toJsonNode(idx.JsonNodeImpl, occupiedVersion)
    x.data.add((n: result, value: value))
    x.freeHead = slot.idx
    slot = toJsonNode(x.data.high.JsonNodeImpl, occupiedVersion)
  else:
    result = toJsonNode(idx.JsonNodeImpl, 1)
    x.data.add((n: result, value: value))
    x.slots.add(toJsonNode(x.data.high.JsonNodeImpl, 1))
    x.freeHead = x.slots.len

proc freeSlot[T](x: var SlotTable[T], slotIdx: int): int {.inline.} =
  # Helper function to add a slot to the freelist. Returns the index that
  # was stored in the slot.
  template slot: untyped = x.slots[slotIdx]
  result = slot.idx
  slot = toJsonNode(x.freeHead.JsonNodeImpl, slot.version + 1)
  x.freeHead = slotIdx

proc delFromSlot[T](x: var SlotTable[T], slotIdx: int) {.inline.} =
  # Helper function to remove a value from a slot and make the slot free.
  # Returns the value deld.
  let valueIdx = x.freeSlot(slotIdx)
  # Remove values/slot_indices by swapping to end.
  x.data[valueIdx] = move(x.data[x.data.high])
  x.data.shrink(x.data.high)
  # Did something take our place? Update its slot to new position.
  if x.data.len > valueIdx:
    let kIdx = x.data[valueIdx].n.idx
    template slot: untyped = x.slots[kIdx]
    slot = toJsonNode(valueIdx.JsonNodeImpl, slot.version)

proc del*[T](x: var SlotTable[T], n: JsonNode) =
  if x.contains(n):
    x.delFromSlot(n.idx)

proc clear*[T](x: var SlotTable[T]) =
  x.freeHead = 0
  x.slots.shrink(0)
  x.data.shrink(0)

template get(x, n) =
  template slot: untyped = x.slots[n.idx]
  if n.idx >= x.slots.len or slot.version != n.version:
    raise newException(KeyError, "JsonNode not in SlotTable")
  # This is safe because we only store valid indices.
  let idx = slot.idx
  result = x.data[idx].value

proc `[]`*[T](x: SlotTable[T], n: JsonNode): T =
  get(x, n)

proc `[]`*[T](x: var SlotTable[T], n: JsonNode): var T =
  get(x, n)

iterator pairs*[T](x: SlotTable[T]): Entry[T] =
  for i in 0 ..< x.len:
    yield x.data[i]
