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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGTASKS_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGTASKS_HPP_
#include <Util/Logger/Logger.hpp>
#include <cinttypes>
#include <memory>
#include <vector>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief This task models the merge task of an a specific slice, with a start and a end.
 */
template<typename SliceType>
struct SliceMergeTask {
    uint64_t sequenceNumber;
    uint64_t startSlice;
    uint64_t endSlice;
    std::vector<std::shared_ptr<SliceType>> slices;
    ~SliceMergeTask() { NES_DEBUG("~SliceMergeTask {}-{}-{}", startSlice, endSlice, sequenceNumber); }
};

}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGTASKS_HPP_
