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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/OperatorHandlerInterfaces/JoinOperatorHandlerInterfaceBucketing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamJoinOperatorHandlerBucketing : public JoinOperatorHandlerInterfaceBucketing,
                                           virtual public StreamJoinOperatorHandler {
  public:
    std::vector<StreamSlice*>* getAllWindowsToFillForTs(uint64_t ts, uint64_t workerId) override;
    std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice) override;
    void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) override;
    StreamJoinOperatorHandlerBucketing(const std::vector<OriginId>& inputOrigins,
                                       const OriginId outputOriginId,
                                       const uint64_t windowSize,
                                       const uint64_t windowSlide,
                                       uint64_t sizeOfRecordLeft,
                                       uint64_t sizeOfRecordRight);

    std::string toString() const override {
        return fmt::format("StreamJoinOperatorHandlerBucketing: windowSize: {}, windowSlide: {}, sizeOfRecordLeft: {}, "
                           "sizeOfRecordRight: {}",
                           windowSize,
                           windowSlide,
                           sizeOfRecordLeft,
                           sizeOfRecordRight);
    }

  private:
    std::vector<std::vector<StreamSlice*>> windowsToFill;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_
