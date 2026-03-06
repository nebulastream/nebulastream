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
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Nautilus/Interface/Hash/BloomFilterRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct BloomFilterMetrics;

/// This class represents a single slice for the NestedLoopJoin. It stores all tuples for the left and right stream.
class NLJSlice final : public Slice
{
public:
    NLJSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfWorkerThreads, BloomFilterMetrics* metrics = nullptr);

    /// Set join key field names for BloomFilter hashing (called before combinePagedVectors)
    void setJoinKeyFieldNames(const std::vector<std::string>& leftKeyFields, const std::vector<std::string>& rightKeyFields);

    /// Returns the number of tuples in this slice on either side.
    [[nodiscard]] uint64_t getNumberOfTuplesLeft() const;
    [[nodiscard]] uint64_t getNumberOfTuplesRight() const;

    /// Returns the pointer to the PagedVector on either side.
    [[nodiscard]] PagedVector* getPagedVectorRefLeft(WorkerThreadId workerThreadId) const;
    [[nodiscard]] PagedVector* getPagedVectorRefRight(WorkerThreadId workerThreadId) const;
    [[nodiscard]] PagedVector* getPagedVectorRef(WorkerThreadId workerThreadId, JoinBuildSideType joinBuildSide) const;

    /// Moves all tuples in this slice to the PagedVector at 0th index on both sides.
    /// Also creates BloomFilters for both sides to optimize join probing (if enabled).
    void combinePagedVectors(bool bloomFilterEnabled = true);

    /// Returns the BloomFilter for the specified join side (may be null if not built)
    [[nodiscard]] Nautilus::Interface::BloomFilter* getBloomFilter(JoinBuildSideType joinBuildSide) const;

    /// Returns a Nautilus-native BloomFilterRef for use in JIT-compiled code.
    [[nodiscard]] Nautilus::Interface::BloomFilterRef getBloomFilterRef(JoinBuildSideType joinBuildSide) const;

    /// This method is called from the BUILD phase for each inserted record
    void addToBloomFilter(uint64_t hash, JoinBuildSideType buildSide);

    // TODO: Future optimization - currently unused but prepared for hash reuse
    // in PROBE phase to avoid redundant hash computations
    /// Stores precomputed join-key hashes per side for later probe optimization.
    void storeJoinKeyHash(uint64_t hash, JoinBuildSideType buildSide);

    /// Expose stored hashes for subsequent integration steps.
    [[nodiscard]] const std::vector<uint64_t>& getLeftJoinKeyHashes() const;
    [[nodiscard]] const std::vector<uint64_t>& getRightJoinKeyHashes() const;

private:
    std::vector<std::unique_ptr<PagedVector>> leftPagedVectors;
    std::vector<std::unique_ptr<PagedVector>> rightPagedVectors;
    std::mutex combinePagedVectorsMutex;

    /// Join key field names for BloomFilter hashing
    std::vector<std::string> leftJoinKeyFields;
    std::vector<std::string> rightJoinKeyFields;

    /// Stored precomputed join-key hashes gathered during BUILD phase.
    std::vector<uint64_t> leftJoinKeyHashes;
    std::vector<uint64_t> rightJoinKeyHashes;

    /// BloomFilters for left and right side (created during combinePagedVectors)
    std::unique_ptr<Nautilus::Interface::BloomFilter> leftBloomFilter;
    std::unique_ptr<Nautilus::Interface::BloomFilter> rightBloomFilter;

    
    /// Metrics tracking pointer (optional, may be null)
    BloomFilterMetrics* bloomFilterMetrics;
};
}
