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

#include <Optimizer/Phases/SampleCodeGenerationPhase.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

SampleCodeGenerationPhase::SampleCodeGenerationPhase() {
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
    queryCompiler = QueryCompilation::NautilusQueryCompiler::create(queryCompilationOptions, phaseFactory, false);
}

SampleCodeGenerationPhasePtr SampleCodeGenerationPhase::create() {
    return std::make_shared<SampleCodeGenerationPhase>(SampleCodeGenerationPhase());
}

QueryPlanPtr SampleCodeGenerationPhase::execute(const QueryPlanPtr&) {
    //TODO: add logic as part of the issue #3890
    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Optimizer
