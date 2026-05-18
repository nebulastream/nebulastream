# `Reflectable` PoC — what gets simpler

Proof-of-concept on top of branch `lr/serialization-registry`. Migrates
`SelectionLogicalOperator` to a generic reflection path; all 14 LogicalPlan
unit tests + all 33 Selection systests pass (full serialize → deserialize →
execute round-trip).

## What a plugin author writes — before vs. after

### Before (current PR)

```cpp
// SelectionLogicalOperator.hpp
class SelectionLogicalOperator {
    /* ... behavior ... */
private:
    LogicalFunction predicate;
    /* ... */
    friend Reflector<TypedLogicalOperator<SelectionLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<SelectionLogicalOperator>> {
    Reflected operator()(const TypedLogicalOperator<SelectionLogicalOperator>&) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SelectionLogicalOperator>> {
    TypedLogicalOperator<SelectionLogicalOperator>
    operator()(const Reflected&, const ReflectionContext&) const;
};

namespace NES::detail {
struct ReflectedSelectionLogicalOperator { LogicalFunction predicate; };
}
```

```cpp
// SelectionLogicalOperator.cpp
Reflected Reflector<TypedLogicalOperator<SelectionLogicalOperator>>::operator()(
    const TypedLogicalOperator<SelectionLogicalOperator>& op) const {
    return reflect(detail::ReflectedSelectionLogicalOperator{op->getPredicate()});
}

TypedLogicalOperator<SelectionLogicalOperator>
Unreflector<TypedLogicalOperator<SelectionLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& ctx) const {
    auto [predicate] = ctx.unreflect<detail::ReflectedSelectionLogicalOperator>(rfl);
    return TypedLogicalOperator<SelectionLogicalOperator>{SelectionLogicalOperator(predicate)};
}
```

### After (this PoC)

```cpp
// SelectionLogicalOperator.hpp
class SelectionLogicalOperator {
public:
    struct Wire { LogicalFunction predicate; };
    Wire wire() const { return Wire{predicate}; }
    static SelectionLogicalOperator fromWire(Wire w, const ReflectionContext&) {
        return SelectionLogicalOperator{std::move(w.predicate)};
    }
    /* ... behavior ... */
};
```

```cpp
// SelectionLogicalOperator.cpp
// — no reflection code at all —
```

## What changed for the author

| | Before | After |
| --- | --- | --- |
| Template specializations to write | 2 | 0 |
| Shadow structs (`detail::ReflectedX`) | 1 | 0 |
| `friend` declarations | 1 | 0 |
| Places the field list is repeated | 3 (class, shadow struct, ctor call in Unreflector) | 1 (Wire struct) |
| Type-level knowledge required | `Reflector<T>`, `Unreflector<T>`, `TypedLogicalOperator<X>`, `Reflected`, `ReflectionContext`, `detail::ReflectedX` convention | `Wire`, `wire()`, `fromWire()` |

Wire shape is co-located with the class — adding a field means editing
one struct, not three coordinated places. The author never touches
`Reflector`, `Unreflector`, or `TypedLogicalOperator<X>`.

## What changed under the hood

A single concept + four template instantiations replace N per-operator
specializations.

```cpp
// nes-common/include/Util/Reflection/Reflectable.hpp — new, ~50 lines
template <typename T>
concept Reflectable = requires(const T& t, const ReflectionContext& ctx) {
    typename T::Wire;
    { t.wire() } -> std::same_as<typename T::Wire>;
    { T::fromWire(std::declval<typename T::Wire>(), ctx) } -> std::same_as<T>;
};

template <Reflectable T> struct Reflector<T>   { /* delegate via wire()    */ };
template <Reflectable T> struct Unreflector<T> { /* delegate via fromWire */ };
```

```cpp
// nes-logical-operators/include/Operators/LogicalOperator.hpp — 1 generic spec each
template <typename T> requires Reflectable<T>
struct Reflector<TypedLogicalOperator<T>>   { /* one impl, 1 line  */ };

template <typename T> requires Reflectable<T>
struct Unreflector<TypedLogicalOperator<T>> { /* one impl, 1 line  */ };
```

For the SelectionLogicalOperator migration this nets `+46 / -35` lines —
because the framework is amortized into one operator. Per additional
operator migrated the cost is roughly `-30` lines (4 specializations + 1
shadow struct + 1 friend, minus one ~10-line `Wire`/`wire`/`fromWire`
block).

## General code-cleanliness wins

- **Single source of truth for the wire shape.** Today the field list
  lives in (1) the class, (2) `detail::ReflectedX`, (3) the reconstruction
  call inside `Unreflector<TypedLogicalOperator<X>>`. Drift between them
  is a silent bug. `Wire` collapses these to one.
- **No `friend` plumbing.** Wire returns a public snapshot; nothing in the
  reflection layer needs private access.
- **The `OperatorId`-on-copy invariant lives in one place.** Today the
  comment-worthy footgun (every implicit conversion `X →
  TypedLogicalOperator<X>` allocates a fresh id, restored externally via
  `withOperatorId`) is implicit in 14 nearly-identical specializations.
  In the PoC it's documented once next to the generic
  `Reflector<TypedLogicalOperator<T>>`.
- **The `detail::ReflectedX` shadow-struct convention disappears.** Today
  it exists *because* you can't make the class itself an aggregate
  (constructors do field validation). `Wire` plays the same role but is
  nested inside the class, named consistently, and discovered via the
  concept.
- **Hard to register-but-forget.** `Reflectable<T>` is a compile-time
  contract; missing a method is a substitution failure at the operator's
  use site, not a runtime missing-key dispatch failure.

## What this does *not* change

- The `LogicalOperatorRegistry` factory shim
  (`RegisterSelectionLogicalOperator`) is untouched. The shim still
  exists; it now calls `unreflect<TypedLogicalOperator<X>>(...)` and
  rides on the generic instead of a per-operator specialization.
- The id-on-copy invariant itself isn't fixed — id restoration still
  happens externally in `QueryPlanSerializationUtil::deserializeQueryPlan`.
  This PoC just stops needing 14 per-operator specs to work around it.
- The two-registry coexistence
  ([BaseRegistry vs UnreflectionRegistry](claude-review.md#6-align-the-accidental-convention-drift-between-the-two-registries))
  is orthogonal and unchanged.

## Caveat surfaced by the build

The original PR could hide the field-recursion in per-operator `.cpp`
files (so `LogicalFunctionReflection.hpp` only needed local visibility).
With the generic `Reflector<TypedLogicalOperator<T>>` body inline in
`LogicalOperator.hpp`, every operator's header must transitively expose
the reflectors for whatever lives in its `Wire`. Concretely, that meant
adding `#include <Serialization/LogicalFunctionReflection.hpp>` to
`SelectionLogicalOperator.hpp`. This is the same pattern a header would
need today if it exposed a `std::vector<T>` member with a custom `T` —
expected behavior, but worth flagging.

## Verification

```
$ docker … nes-logical-plan-test
[==========] 14 tests from 1 test suite ran. (1 ms total)
[  PASSED  ] 14 tests.

$ docker … nes-projection-logical-operator-test
[==========] 9 tests from 1 test suite ran. (1 ms total)
[  PASSED  ] 9 tests.   # includes the explicit
                       # *SurvivesReflectionRoundTrip cases

$ docker … systest -t nes-systests/operator/selection
All queries passed.    # 33/33, full serialize → deserialize → execute
Total execution time: 2300 ms
```

Files touched: `nes-common/include/Util/Reflection.hpp` (1 added line),
`nes-common/include/Util/Reflection/Reflectable.hpp` (new, ~50 lines),
`nes-logical-operators/include/Operators/LogicalOperator.hpp` (+29
generic Reflector/Unreflector + comment),
`nes-logical-operators/include/Operators/SelectionLogicalOperator.hpp`
(-19 / +14 wire pattern), `nes-logical-operators/src/Operators/
SelectionLogicalOperator.cpp` (-13 specialization bodies).
