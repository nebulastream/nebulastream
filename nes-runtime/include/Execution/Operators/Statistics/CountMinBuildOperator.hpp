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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_COUNTMINBUILDOPERATOR_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_COUNTMINBUILDOPERATOR_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <memory>
#include <vector>

namespace NES {

namespace Runtime::Execution {
class RecordBuffer;

namespace Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;
}// namespace Operators
}// namespace Runtime::Execution

namespace Runtime::Execution::MemoryProvider {
class MemoryProvider;
using MemoryProviderPtr = std::unique_ptr<MemoryProvider>;
}// namespace Runtime::Execution::MemoryProvider

namespace Nautilus::Interface {
class HashFunction;
}

namespace Experimental::Statistics {

/**
 * @brief a class that implements the CountMinBuild operator used to construct CountMin sketches
 */
class CountMinBuildOperator : public Runtime::Execution::Operators::ExecutableOperator {
  public:
    /**
     * @param operatorHandlerIndex the index of the CountMinOperatorHandler
     * @param width the width of the CountMin sketch
     * @param depth the depth of the CountMin sketch
     * @param onField the field name from which the CountMinBuildOperator reads the data
     * @param keySizeInBits the size of the keys in bits
     * @param timeFunction the function defining which time is used (event or ingestion)
     * @param schema used to create the RowMemoryProvider, which allows us to read and write tuples from and to RecordBuffers
     */
    CountMinBuildOperator(uint64_t operatorHandlerIndex,
                          uint64_t width,
                          uint64_t depth,
                          const std::string& onField,
                          uint64_t keySizeInBits,
                          Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
                          SchemaPtr schema);

    /**
     * @brief gets the BufferManager from the execution context and sets it for the CountMinOperatorHandler
     * @param ctx the execution context
     */
    void setup(Runtime::Execution::ExecutionContext& ctx) const override;

    /**
     * @brief takes a incomingRecord and modifies a sketch accordingly
     * @param ctx the execution context
     * @param incomingRecord the incomingRecord that is used to modify the CountMin sketch
     */
    void execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& incomingRecord) const override;

    /**
     * @brief is called for each RecordBuffer that is processed. The watermark is extracted and the finished windows constructing
     * CountMin records are dispatched to the next operator in the pipeline as TupleBuffers
     * @param ctx the execution context
     * @param recordBuffer the record buffer that has been fully processed
     */
    void close(Runtime::Execution::ExecutionContext& ctx, Runtime::Execution::RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const uint64_t width;
    const uint64_t depth;
    const std::string onField;
    const uint64_t keySizeInBits;
    const Runtime::Execution::Operators::TimeFunctionPtr timeFunction;
    const Runtime::Execution::MemoryProvider::MemoryProviderPtr memoryProvider;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_COUNTMINBUILDOPERATOR_HPP_
