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
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>

namespace NES::QueryCompilation::Phases
{

class DefaultPhaseFactory : public PhaseFactory
{
public:
    ~DefaultPhaseFactory() override = default;
    static std::shared_ptr<PhaseFactory> create();
    std::shared_ptr<LowerLogicalToPhysicalOperators>
    createLowerLogicalQueryPlanPhase(Configurations::QueryCompilerConfiguration queryCompilerConfig) const override;
    [[nodiscard]] std::shared_ptr<PipeliningPhase> createPipeliningPhase() const override;
    [[nodiscard]] std::shared_ptr<AddScanAndEmitPhase>
    createAddScanAndEmitPhase(Configurations::QueryCompilerConfiguration queryCompilerConfig) const override;
};

}
