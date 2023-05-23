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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <mutex>
#include <utility>

namespace NES::Runtime::Execution::Operators {
/**
* @brief This handler stores states of the out-of-order-ratio operator during its execution
*/
class OutOfOrderRatioOperatorHandler : public OperatorHandler {
  public:
    explicit OutOfOrderRatioOperatorHandler() {}

    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}

    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

    uint64_t numberOfRecords = 0;
    uint64_t numberOfOutOfOrderRecords = 0;
    uint64_t lastRecordTs = 0;
    std::mutex mutex;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATORHANDLER_HPP_