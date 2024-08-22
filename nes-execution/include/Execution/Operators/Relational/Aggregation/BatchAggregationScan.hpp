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
#include <memory>
#include <vector>
#include <stdint.h>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

#include <Execution/Operators/Operator.hpp>

namespace NES
{
namespace Runtime
{
namespace Execution
{
class ExecutionContext;
class RecordBuffer;
namespace Aggregation
{
class AggregationFunction;
} /// namespace Aggregation
} /// namespace Execution
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief Batch Aggregation scan operator, it accesses multiple thread local states and performs the final aggregation.
 */
class BatchAggregationScan : public Operator
{
public:
    /**
     * @brief Creates a batch aggregation operator with a expression.
     */
    BatchAggregationScan(
        uint64_t operatorHandlerIndex,
        const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions);
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

private:
    const uint64_t operatorHandlerIndex;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
};
} /// namespace NES::Runtime::Execution::Operators
