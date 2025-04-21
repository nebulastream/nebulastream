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

<<<<<<<< HEAD:nes-logical-operators/src/Operators/LogicalOperator.cpp
#include <memory>
#include <Operators/LogicalOperator.hpp>
========
#include <Plans/PipelineQueryPlan.hpp>
#include <ExecutableQueryPlan.hpp>
>>>>>>>> 18c6cfc0a5 (refactor(QueryCompiler): move into):nes-query-compiler/include/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp

namespace NES
{
<<<<<<<< HEAD:nes-logical-operators/src/Operators/LogicalOperator.cpp

LogicalOperator::LogicalOperator(std::shared_ptr<LogicalOperator> logicalOp) : outputSchema(logicalOp->outputSchema)
{
};

========
std::unique_ptr<ExecutableQueryPlan> apply(const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan);
>>>>>>>> 18c6cfc0a5 (refactor(QueryCompiler): move into):nes-query-compiler/include/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp
}
