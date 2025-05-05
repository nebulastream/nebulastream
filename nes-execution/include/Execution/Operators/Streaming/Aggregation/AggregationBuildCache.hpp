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
#include <cstdint>
#include <memory>
#include <vector>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>

namespace NES::Runtime::Execution::Operators
{

class AggregationBuildCache final : public WindowAggregationOperator, public WindowOperatorBuild
{
public:
    AggregationBuildCache(
        uint64_t operatorHandlerIndex,
        std::unique_ptr<TimeFunction> timeFunction,
        std::vector<std::unique_ptr<Functions::Function>> keyFunctions,
        WindowAggregationOperator windowAggregationOperator,
        QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions);

    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;

private:
    const std::vector<std::unique_ptr<Functions::Function>> keyFunctions;

    /// This might not be the best place to store it, but it is an easy way to use them in this PoC branch
    QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions;
};

}
