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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERFACES_JOINOPERATORHANDLERINTERFACESLICING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERFACES_JOINOPERATORHANDLERINTERFACESLICING_HPP_

namespace NES::Runtime::Execution::Operators {
class JoinOperatorHandlerInterfaceSlicing {
  public:
    /**
     * @brief Retrieves the slice that corresponds to the timestamp. If no window exists for the timestamp, one gets created
     * @param timestamp
     * @return StreamSlicePtr
     */
    virtual StreamSlicePtr getSliceByTimestampOrCreateIt(uint64_t timestamp) = 0;

    /**
     * @brief Returns the current window, by current we mean the last added window to the list. If no slice exists,
     * a slice with the timestamp 0 will be created
     * @return StreamSlice*
     */
    virtual StreamSlice* getCurrentWindowOrCreate() = 0;

    /**
     * @brief Returns the left and right slices for a given window, so that all tuples are being joined together
     * @param windowId
     * @return Vector<Pair<LeftSlice, RightSlice>>
     */
    virtual std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>> getSlicesLeftRightForWindow(uint64_t windowId) = 0;

};
} // namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERFACES_JOINOPERATORHANDLERINTERFACESLICING_HPP_
