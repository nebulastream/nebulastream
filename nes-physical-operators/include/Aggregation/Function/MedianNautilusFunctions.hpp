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

#include <cstdint>
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The bodies that MedianAggregationPhysicalFunction registers as shared nautilus functions instead of inlining
/// them at every aggregation. medianOf is the expensive one: it is a doubly-nested loop, so a query with many
/// MEDIANs would otherwise inline that loop once per aggregation.
///
/// They are free functions over a runtime layout and nullability rather than members, so that one traced instance
/// can serve every median of the same shape in a pipeline. medianOf reads the stored value by name instead of
/// applying the column-specific input function, which is what makes it shareable at all.
nautilus::val<double> medianOf(
    const nautilus::val<TupleBuffer*>& pagedVectorBuffer,
    const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
    const Record::RecordFieldIdentifier& valueFieldName);

/// Merges the pages of the second state into the first. Null flag semantics follow the SQL standard: the state
/// stays null only while neither side has seen a non-null value.
void medianCombineStates(
    const nautilus::val<AggregationState*>& aggregationState1,
    const nautilus::val<AggregationState*>& aggregationState2,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    bool nullable);

/// Allocates a fresh paged vector for the state and marks it as "no value seen yet".
void medianResetState(
    const nautilus::val<AggregationState*>& aggregationState,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<uint64_t>& tupleSize,
    bool nullable);

}
