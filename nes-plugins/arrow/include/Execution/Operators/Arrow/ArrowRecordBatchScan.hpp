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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_SCAN_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_SCAN_HPP_

#include <Execution/Operators/Operator.hpp>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution::Operators {

class AbstractArrowFieldReader;

/**
 * @brief This scan operator extracts records from a arrow record batch.
 * To this end, it assumes that it receives a RecordBuffer that points to a RecordBufferWrapper object.
 * Furthermore, it supports projection pushdown to eliminate unneeded reads.
 */
class ArrowRecordBatchScan : public Operator {
  public:
    /**
     * @brief Constructor for the arrow scan operator that receives a list of AbstractArrowFieldReader
     * @param std::vector<std::shared_ptr<AbstractArrowFieldReader>>
     */
    ArrowRecordBatchScan(const std::vector<std::shared_ptr<AbstractArrowFieldReader>>& readers);

    /**
     * @brief Constructor for the arrow scan operator that creates a list of AbstractArrowFieldReader from a Schema
     * @param schema
     */
    ArrowRecordBatchScan(const SchemaPtr& schema);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    const std::vector<std::shared_ptr<AbstractArrowFieldReader>> readers;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_SCAN_HPP_
