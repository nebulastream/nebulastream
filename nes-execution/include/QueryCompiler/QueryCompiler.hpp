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
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPhase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <CompiledQueryPlan.hpp>

namespace NES::QueryCompilation
{
class QueryCompiler
{
public:
    QueryCompiler(Configurations::QueryCompilerConfiguration queryCompilerConfig, const Phases::PhaseFactory& phaseFactory);
    std::unique_ptr<Runtime::Execution::CompiledQueryPlan> compileQuery(const std::shared_ptr<QueryCompilationRequest>& request) const;

protected:
    Configurations::QueryCompilerConfiguration queryCompilerConfig;
    std::shared_ptr<LowerLogicalToPhysicalOperators> lowerLogicalToPhysicalOperatorsPhase;
    std::shared_ptr<LowerPhysicalToNautilusOperators> lowerPhysicalToNautilusOperatorsPhase;
    std::shared_ptr<NautilusCompilationPhase> compileNautilusPlanPhase;
    std::shared_ptr<PipeliningPhase> pipeliningPhase;
    std::shared_ptr<MemoryLayoutSelectionPhase> memoryLayoutSelectionPhase;
    std::shared_ptr<AddScanAndEmitPhase> addScanAndEmitPhase;
};

}
