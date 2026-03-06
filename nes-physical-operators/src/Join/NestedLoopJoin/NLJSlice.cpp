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

#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <fstream>

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <SliceStore/Slice.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

NLJSlice::NLJSlice(const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads, BloomFilterMetrics* metrics) 
    : Slice(sliceStart, sliceEnd), bloomFilterMetrics(metrics)
{
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        leftPagedVectors.emplace_back(std::make_unique<PagedVector>());
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        rightPagedVectors.emplace_back(std::make_unique<PagedVector>());
    }
}

void NLJSlice::setJoinKeyFieldNames(const std::vector<std::string>& leftKeyFields,
                                    const std::vector<std::string>& rightKeyFields)
{
    leftJoinKeyFields = leftKeyFields;
    rightJoinKeyFields = rightKeyFields;
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectors.begin(),
        leftPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectors.begin(),
        rightPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectors.size();
    return leftPagedVectors[pos].get();
}

PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectors.size();
    return rightPagedVectors[pos].get();
}

PagedVector* NLJSlice::getPagedVectorRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Right:
            return getPagedVectorRefRight(workerThreadId);
        case JoinBuildSideType::Left:
            return getPagedVectorRefLeft(workerThreadId);
    }
    std::unreachable();
}

void NLJSlice::combinePagedVectors(const bool bloomFilterEnabled)
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectors.size(); ++i)
        {
            leftPagedVectors[0]->moveAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i)
        {
            rightPagedVectors[0]->moveAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }
    
    if (bloomFilterEnabled)
    {
        
        // Track BloomFilter creation in metrics (BUILD phase tracking)
        if (bloomFilterMetrics && (leftBloomFilter || rightBloomFilter)) {
            NES_INFO("BloomFilter BUILD: leftBF={} bits, rightBF={} bits, slice=[{}, {}]",
                     leftBloomFilter ? leftBloomFilter->sizeInBits() : 0,
                     rightBloomFilter ? rightBloomFilter->sizeInBits() : 0,
                     sliceStart, sliceEnd);
        }
    }
}


Nautilus::Interface::BloomFilter* NLJSlice::getBloomFilter(const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Left:
            return leftBloomFilter.get();
        case JoinBuildSideType::Right:
            return rightBloomFilter.get();
    }
    std::unreachable();
}

Nautilus::Interface::BloomFilterRef NLJSlice::getBloomFilterRef(const JoinBuildSideType joinBuildSide) const
{
    return Nautilus::Interface::BloomFilterRef(getBloomFilter(joinBuildSide));
}

void NLJSlice::storeJoinKeyHash(const uint64_t hash, const JoinBuildSideType buildSide)
{
    const std::scoped_lock lock(combinePagedVectorsMutex);

    switch (buildSide)
    {
        case JoinBuildSideType::Left:
            leftJoinKeyHashes.push_back(hash);
            break;
        case JoinBuildSideType::Right:
            rightJoinKeyHashes.push_back(hash);
            break;
    }
}

const std::vector<uint64_t>& NLJSlice::getLeftJoinKeyHashes() const
{
    return leftJoinKeyHashes;
}

const std::vector<uint64_t>& NLJSlice::getRightJoinKeyHashes() const
{
    return rightJoinKeyHashes;
}

void NLJSlice::addToBloomFilter(const uint64_t hash, const JoinBuildSideType buildSide)
{
    // Initialize BloomFilters on first use (lazy initialization during BUILD phase)
    constexpr double falsePositiveRate = 0.01;
    const std::scoped_lock lock(combinePagedVectorsMutex);
    
    switch (buildSide)
    {
        case JoinBuildSideType::Left:
            leftJoinKeyHashes.push_back(hash);
            if (!leftBloomFilter)
            {
                // TODO: Improve for dynamic sizing: currently we start with a reasonable default size, but in the future we could use the number of inserted hashes to resize the BloomFilter dynamically during BUILD phase
                // Estimate: start with reasonable size, will hold 100K elements
                leftBloomFilter = std::make_unique<Nautilus::Interface::BloomFilter>(100000, falsePositiveRate);
            }
            leftBloomFilter->add(hash);
            break;
            
        case JoinBuildSideType::Right:
            rightJoinKeyHashes.push_back(hash);
            if (!rightBloomFilter)
            {
                rightBloomFilter = std::make_unique<Nautilus::Interface::BloomFilter>(100000, falsePositiveRate);
            }
            rightBloomFilter->add(hash);
            break;
    }
}

} // namespace NES
