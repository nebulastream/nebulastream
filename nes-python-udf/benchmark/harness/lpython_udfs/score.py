from lpython import i32, i64, f64
from numpy import empty, float64, exp

def score(a: i64, b: i64) -> f64:
    weights: f64[2] = empty(2, dtype=float64)
    weights[0] = 0.5
    weights[1] = -0.25
    values: f64[2] = empty(2, dtype=float64)
    values[0] = f64(a)
    values[1] = f64(b)
    products: f64[2] = weights * values
    z: f64 = 0.0
    i: i32
    for i in range(2):
        z = z + products[i]
    return 1.0 / (1.0 + exp(-z))

print(score(i64(1), i64(2)))
