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

#include <memory>
#include <utility>
#include <HashMapOptions.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Typed handle over the anonymous NautilusBuffer that a SliceStoreRef hands out, naming what the buffer actually backs.
/// A given slice buffer is statically known (per operator) to back a ChainedHashMap (hash join / aggregation) or a PagedVector
/// (nested loop join). These wrappers make that meaning explicit at the operator boundary and hide the `asArg()` plumbing behind a
/// named factory, so the operator constructs the concrete ref without spelling out a raw `nautilus::val<TupleBuffer*>`.
///
/// The handle owns the NautilusBuffer; the ref it produces borrows into that buffer, so the handle must outlive the ref. Store it in a
/// named local and never call the factories on a temporary (the `&&` overloads are deleted to enforce this, mirroring NautilusBuffer).

/// A slice buffer known to back a ChainedHashMap.
class HashMapSliceBuffer
{
    NautilusBuffer buffer;

public:
    explicit HashMapSliceBuffer(NautilusBuffer buffer) : buffer(std::move(buffer)) { }

    /// Interprets the slice buffer as a chained hash map.
    [[nodiscard]] ChainedHashMapRef asHashMap(const HashMapOptions& options) const&
    {
        return ChainedHashMapRef{
            buffer.asArg(), options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize};
    }
    ChainedHashMapRef asHashMap(const HashMapOptions&) const&& = delete;

    /// Raw TupleBuffer pointer for the hash-map-internal operations that still need it (entry refs, child-buffer invokes).
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const& { return buffer.asArg(); }
    nautilus::val<const TupleBuffer*> asArg() const&& = delete;
};

/// A slice buffer known to back a PagedVector.
class PagedVectorSliceBuffer
{
    NautilusBuffer buffer;

public:
    explicit PagedVectorSliceBuffer(NautilusBuffer buffer) : buffer(std::move(buffer)) { }

    /// Interprets the slice buffer as a paged vector.
    [[nodiscard]] PagedVectorRef asPagedVector(const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout) &
    {
        return PagedVectorRef{BorrowedNautilusBuffer::from(buffer.asArg()), tupleLayout};
    }
    PagedVectorRef asPagedVector(const std::shared_ptr<PagedVectorTupleLayout>&) && = delete;
};

}
