import jsonnodeids

const
  defaultInitialCap = 64

type
  Entry*[T] = tuple
    n: JsonNodeId
    value: T

  SlotTable*[T] = object
    freeHead: int
    slots: seq[JsonNodeId]
    data: seq[Entry[T]]

proc newSlotTableOfCap*[T](cap = defaultInitialCap.Natural): SlotTable[T] =
  result = SlotTable[T](
    data: newSeqOfCap[Entry[T]](cap),
    slots: newSeqOfCap[JsonNodeId](cap),
    freeHead: 0
  )

proc len*[T](x: SlotTable[T]): int {.inline.} =
  x.data.len

proc contains*[T](x: SlotTable[T], n: JsonNodeId): bool {.inline.} =
  n.idx < x.slots.len and x.slots[n.idx].version == n.version

proc raiseRangeDefect() {.noinline, noreturn.} =
  raise newException(RangeDefect, "SlotTable number of elements overflow")

proc incl*[T](x: var SlotTable[T], value: T): JsonNodeId =
  when compileOption("boundChecks"):
    if x.len + 1 == maxJsonNodes:
      raiseRangeDefect()
  let idx = x.freeHead
  if idx < x.slots.len:
    template slot: untyped = x.slots[idx]
    let occupiedVersion = slot.version or 1
    result = toJsonNodeId(idx.JsonNodeIdImpl, occupiedVersion)
    x.data.add((n: result, value: value))
    x.freeHead = slot.idx
    slot = toJsonNodeId(x.data.high.JsonNodeIdImpl, occupiedVersion)
  else:
    result = toJsonNodeId(idx.JsonNodeIdImpl, 1)
    x.data.add((n: result, value: value))
    x.slots.add(toJsonNodeId(x.data.high.JsonNodeIdImpl, 1))
    x.freeHead = x.slots.len

proc freeSlot[T](x: var SlotTable[T], slotIdx: int): int {.inline.} =
  # Helper function to add a slot to the freelist. Returns the index that
  # was stored in the slot.
  template slot: untyped = x.slots[slotIdx]
  result = slot.idx
  slot = toJsonNodeId(x.freeHead.JsonNodeIdImpl, slot.version + 1)
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
    slot = toJsonNodeId(valueIdx.JsonNodeIdImpl, slot.version)

proc del*[T](x: var SlotTable[T], n: JsonNodeId) =
  if x.contains(n):
    x.delFromSlot(n.idx)

proc clear*[T](x: var SlotTable[T]) =
  x.freeHead = 0
  x.slots.shrink(0)
  x.data.shrink(0)

proc raiseKeyError {.noinline, noreturn.} =
  raise newException(KeyError, "JsonNodeId not in SlotTable")

template get(x, n) =
  template slot: untyped = x.slots[n.idx]
  if n.idx >= x.slots.len or slot.version != n.version:
    raiseKeyError()
  # This is safe because we only store valid indices.
  let idx = slot.idx
  result = x.data[idx].value

proc `[]`*[T](x: SlotTable[T], n: JsonNodeId): T =
  get(x, n)

proc `[]`*[T](x: var SlotTable[T], n: JsonNodeId): var T =
  get(x, n)

iterator pairs*[T](x: SlotTable[T]): Entry[T] =
  for i in 0 ..< x.len:
    yield x.data[i]
