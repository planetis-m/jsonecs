import jsonnodes
from typetraits import supportsCopyMem

type
  DArray*[T] = object
    len: int
    data: ptr UncheckedArray[T]

proc `=destroy`*[T](x: var DArray[T]) =
  if x.data != nil:
    when not supportsCopyMem(T):
      for i in 0..<x.len: `=destroy`(x[i])
    deallocShared(x.data)
proc `=copy`*[T](dest: var DArray[T], src: DArray[T]) {.error.}

proc newDArray*[T](len = 0.Natural): DArray[T] =
  when not supportsCopyMem(T):
    result.data = cast[typeof(result.data)](allocShared0(len * sizeof(T)))
  else:
    result.data = cast[typeof(result.data)](allocShared(len * sizeof(T)))
  result.len = len

proc grow*[T](s: var DArray[T], newLen: Natural) =
  if s.len < newLen:
    when not supportsCopyMem(T):
      s.data = cast[typeof(s.data)](reallocShared0(s.data, s.len * sizeof(T), newLen * sizeof(T)))
    else:
      s.data = cast[typeof(s.data)](reallocShared(s.data, newLen * sizeof(T)))
    s.len = newLen

proc len*[T](s: DArray[T]): int {.inline.} = s.len

proc raiseRangeDefect {.noinline, noreturn.} =
  raise newException(RangeDefect, "array access out of bounds")

template checkArrayAccess() =
  when compileOption("boundChecks"):
    {.line.}:
      if x.data == nil or i >= x.len:
        raiseRangeDefect()

template get(x, i) =
  checkArrayAccess()
  x.data[i]

proc `[]`*[T](x: DArray[T]; i: Natural): lent T =
  get(x, i)
proc `[]`*[T](x: var DArray[T]; i: Natural): var T =
  get(x, i)

proc `[]=`*[T](x: var DArray[T]; i: Natural; y: sink T) =
  checkArrayAccess()
  x.data[i] = y

proc clear*[T](x: DArray[T]) =
  when not supportsCopyMem(T):
    if x.data != nil:
      for i in 0..<x.len: reset(x[i])

template toOpenArray*(x, first, last: typed): untyped =
  toOpenArray(x.data, first, last)
