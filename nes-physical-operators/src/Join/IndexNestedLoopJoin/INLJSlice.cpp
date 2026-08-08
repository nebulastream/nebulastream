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

#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>

#include <cstdint>
#include <Interface/BTree/BTree.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

INLJSlice::INLJSlice(
    AbstractBufferProvider& bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfWorkerThreads,
    const uint64_t tupleSizeLeft,
    const uint64_t tupleSizeRight)
    : Slice(sliceStart, sliceEnd)
{
    const auto mainBufferSize = BTree::getMainBufferSize();
    const auto pageSize = bufferProvider.getBufferSize();
    const auto allocateTrees = [&](std::vector<TupleBuffer>& buffers, const uint64_t tupleSize)
    {
        buffers.reserve(numberOfWorkerThreads);
        for (uint64_t index = 0; index < numberOfWorkerThreads; ++index)
        {
            auto buffer = bufferProvider.getUnpooledBuffer(mainBufferSize);
            if (not buffer.has_value())
            {
                throw BufferAllocationFailure("No unpooled TupleBuffer available for an INLJ BTree main buffer");
            }
            BTree::init(*buffer, pageSize, tupleSize);
            buffers.emplace_back(*buffer);
        }
    };
    allocateTrees(leftBTreeBuffers, tupleSizeLeft);
    allocateTrees(rightBTreeBuffers, tupleSizeRight);
}

const TupleBuffer* INLJSlice::getBTreeBuffer(const uint64_t workerIndex, const JoinBuildSideType side) const
{
    const auto& buffers = side == JoinBuildSideType::Left ? leftBTreeBuffers : rightBTreeBuffers;
    PRECONDITION(workerIndex < buffers.size(), "INLJ worker index {} out of bounds for {} trees", workerIndex, buffers.size());
    return &buffers[workerIndex];
}

uint64_t INLJSlice::getNumberOfBTrees(const JoinBuildSideType side) const
{
    return side == JoinBuildSideType::Left ? leftBTreeBuffers.size() : rightBTreeBuffers.size();
}

uint64_t INLJSlice::getNumberOfTuples(const JoinBuildSideType side) const
{
    const auto& buffers = side == JoinBuildSideType::Left ? leftBTreeBuffers : rightBTreeBuffers;
    uint64_t result = 0;
    for (const auto& buffer : buffers)
    {
        result += BTree::load(buffer).size();
    }
    return result;
}

}
