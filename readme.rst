==========================================
               JSON ECS
==========================================

JSON data structure based on the ECS pattern.

.. code-block:: nim

  let testJson = parseJson"""{ "a": [1, 2, {"key": [4, 5]}, 4]}"""
  for (key, node1) in pairs(testJson):
    echo key, ": ", node1.kind
    for node2 in items(node1):
      echo node2.getInt()

Acknowledgements
================

- `packedjson <https://github.com/Araq/packedjson>`_ packedjson is an alternative Nim implementation for JSON.
- `stdlib.json <https://nim-lang.github.io/Nim/json.html>`_
