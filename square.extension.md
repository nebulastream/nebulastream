---
type: function
name: Square
category: ArithmeticalFunctions
---

## Description
Computes the square of the input: x * x.
Accepts any numeric argument and returns the same type as the input.

## Arguments
- children: 1
- input types: numeric
- output type: same as input

## Behavior
`withInferredDataType()`: Verify the single child is numeric. Set `dataType` to the child's data type (output type equals input type).
`execute()` (physical): Call `childFunction.execute(record, arena)`. Compute `value * value`. Cast result to `outputType`. Return as `VarVal`.

## Examples
- SQUARE(3) → 9
- SQUARE(-4) → 16
- SQUARE(2.0) → 4.0
- SQUARE(0) → 0
