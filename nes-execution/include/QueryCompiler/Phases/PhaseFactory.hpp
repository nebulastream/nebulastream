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
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
namespace NES::QueryCompilation::Phases
{

/// An abstract factory, which allows the query compiler to create instances of particular phases,
/// without knowledge about the concrete implementations. This ensures extendability.
class PhaseFactory
{
public:
    virtual LowerLogicalToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(std::shared_ptr<QueryCompilerOptions> options) = 0;
    virtual PipeliningPhasePtr createPipeliningPhase() = 0;
    virtual AddScanAndEmitPhasePtr createAddScanAndEmitPhase(std::shared_ptr<QueryCompilerOptions> options) = 0;
};
using PhaseFactoryPtr = std::shared_ptr<PhaseFactory>;
}
