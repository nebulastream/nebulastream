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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// This class represents a single slice for the NestedLoopJoin. It stores all tuples for the left and right stream.
class NLJSlice final : public Slice
{
public:
    NLJSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfWorkerThreads);

    /// Returns the number of tuples in this slice on either side.
    [[nodiscard]] uint64_t getNumberOfTuplesLeft() const;
    [[nodiscard]] uint64_t getNumberOfTuplesRight() const;

    /// Returns the pointer to the PagedVector on either side.
    [[nodiscard]] Nautilus::Interface::PagedVector* getPagedVectorRefLeft(WorkerThreadId workerThreadId) const;
    [[nodiscard]] Nautilus::Interface::PagedVector* getPagedVectorRefRight(WorkerThreadId workerThreadId) const;

    /// Moves all tuples in this slice to the PagedVector at 0th index on both sides.
    void combinePagedVectors();

private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> leftPagedVectors;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> rightPagedVectors;
    std::mutex combinePagedVectorsMutex;
};
}
