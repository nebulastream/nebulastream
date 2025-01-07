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
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation
{

using LowerPhysicalToNautilusOperatorsPtr = std::shared_ptr<class LowerPhysicalToNautilusOperators>;
using NautilusCompilationPhasePtr = std::shared_ptr<class NautilusCompilationPhase>;

class QueryCompiler

{
public:
    QueryCompiler(const std::shared_ptr<QueryCompilerOptions>& options, const std::shared_ptr<Phases::PhaseFactory>& phaseFactory);
    QueryCompilationResult compileQuery(const QueryCompilationRequestPtr& request);

protected:
    std::shared_ptr<QueryCompilerOptions> options;
    LowerLogicalToPhysicalOperatorsPtr lowerLogicalToPhysicalOperatorsPhase;
    LowerPhysicalToNautilusOperatorsPtr lowerPhysicalToNautilusOperatorsPhase;
    NautilusCompilationPhasePtr compileNautilusPlanPhase;
    PipeliningPhasePtr pipeliningPhase;
    AddScanAndEmitPhasePtr addScanAndEmitPhase;
};

}
