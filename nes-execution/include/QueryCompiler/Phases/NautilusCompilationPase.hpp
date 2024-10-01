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
#include <functional>
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation
{

/// Compilation phase, which generates executable machine code for pipelines of nautilus operators.
class NautilusCompilationPhase
{
public:
    explicit NautilusCompilationPhase(const std::shared_ptr<QueryCompilerOptions>& compilerOptions);

    /// Generates code for all pipelines in a pipelined query plan.
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr queryPlan);

    /// Generates code for a particular pipeline.
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

private:
    std::shared_ptr<QueryCompilerOptions> compilerOptions;
};
}
