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
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <memory>
#include <vector>

namespace NES {
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
     * @param h3Seeds the seeds for the H3 hash functions
     */
    CountMinBuildOperator(uint64_t operatorHandlerIndex,
                          uint64_t width,
                          uint64_t depth,
                          const std::string& onField,
                          uint64_t keySizeInBits,
                          Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
                          std::vector<uint64_t> h3Seeds);

    /**
     * @brief takes a record and modifies a sketch accordingly
     * @param ctx the execution context
     * @param record the record that is used to modify the CountMin sketch
     */
    void execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const uint64_t width;
    const uint64_t depth;
    const std::string onField;
    const uint64_t keySizeInBits;
    const Runtime::Execution::Operators::TimeFunctionPtr timeFunction;
    const std::vector<uint64_t> h3Seeds;
    const Runtime::Execution::MemoryProvider::MemoryProviderPtr memoryProvider;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_COUNTMINBUILDOPERATOR_HPP_
