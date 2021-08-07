import ".." / jsonecs
import strutils, times

proc main =
  let jobj = parseFile("1.json")

  let coordinates = jobj["coordinates"]
  #let len = float(coordinates.len)
  #doAssert coordinates.len == 1000000
  var x = 0.0
  var y = 0.0
  var z = 0.0

  for coord in coordinates.items:
    x += coord["x"].getFloat
    y += coord["y"].getFloat
    z += coord["z"].getFloat

let start = cpuTime()
main()
echo "used Mem: ", formatSize getMaxMem(), " time: ", cpuTime() - start, "s"
