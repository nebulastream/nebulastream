/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <DataTypes/Schema.hpp>

namespace NES
{

/// Expand a logical schema into its primitive-only realisation.
///
/// Each field is looked up in `LogicalTypeLoweringRegistry`. Compound logical
/// types (e.g. `Point`) yield N primitive fields whose names are
/// `<original-name><component-suffix>` (so a `Point` field `p` becomes `p_X`,
/// `p_Y`, `p_Z`); scalar logical types yield a single field with the same
/// name (the registered scalar layout uses an empty suffix).
///
/// The returned schema's per-field `LogicalType` uses the prototype's reduced
/// vocabulary (`INTEGER`, `FLOAT`, `BOOL`, `TEXT`) so it round-trips through
/// `toPhysical`/`SchemaFormatter` with the same names users see in DDL.
///
/// Idempotent for already-flat schemas: scalar types lower to themselves.
Schema lowerSchema(const Schema& logical);

/// Byte size of a single tuple of the given schema once lowered to primitives.
/// Sums `getSizeInBytesWithNull()` of every primitive component (so a `Point`
/// field contributes 24 bytes, not 0). LogicalType itself carries no width;
/// any byte-size question has to round-trip through the lowering registry.
size_t physicalTupleByteSize(const Schema& logical);

}
