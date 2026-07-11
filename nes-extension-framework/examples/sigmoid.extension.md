---
type: function
name: Sigmoid
category: ArithmeticalFunctions
---

## Description

Computes the sigmoid (logistic) activation function: `1 / (1 + e^(-x))`.
Accepts a single floating-point or integer argument and returns a float in the range (0, 1).
Useful for machine-learning-style score normalization inside queries.

## Arguments

- children: 1
- input types: numeric
- output type: float64

## Behavior

`withInferredDataType()`: Verify the single child is numeric. Set `dataType` to `DataType::FLOAT64` regardless of input type (sigmoid always returns float).

`execute()` (physical): Call `childFunction.execute(record, arena)` to get the input value. Cast to float. Compute `1.0f / (1.0f + exp(-value))` using Nautilus IR. Return result as `VarVal`.

## Examples

- sigmoid(0.0) → 0.5
- sigmoid(2.0) → approximately 0.8808
- sigmoid(-2.0) → approximately 0.1192
- sigmoid(100.0) → approximately 1.0 (saturates near 1)
