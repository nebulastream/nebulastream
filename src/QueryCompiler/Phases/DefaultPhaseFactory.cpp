/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/Phases/CodeGenerationPhase.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/NeverFusePolicy.hpp>
#include <QueryCompiler/Phases/PredicationOptimizationPhase.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultGeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToGeneratableOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Logger.hpp>

namespace NES::QueryCompilation::Phases {

PhaseFactoryPtr DefaultPhaseFactory::create() { return std::make_shared<DefaultPhaseFactory>(); }

PipeliningPhasePtr DefaultPhaseFactory::createPipeliningPhase(QueryCompilerOptionsPtr options) {
    if (options->isOperatorFusionEnabled()) {
        NES_DEBUG("Create pipelining phase with fuse policy");
        auto operatorFusionPolicy = FuseNonPipelineBreakerPolicy::create();
        return DefaultPipeliningPhase::create(operatorFusionPolicy);
    }
    NES_DEBUG("Create pipelining phase with always break policy");
    auto operatorFusionPolicy = NeverFusePolicy::create();
    return DefaultPipeliningPhase::create(operatorFusionPolicy);
}

LowerLogicalToPhysicalOperatorsPtr DefaultPhaseFactory::createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr) {
    NES_DEBUG("Create default lower logical plan phase");
    auto physicalOperatorProvider = DefaultPhysicalOperatorProvider::create();
    return LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);
}

AddScanAndEmitPhasePtr DefaultPhaseFactory::createAddScanAndEmitPhase(QueryCompilerOptionsPtr) {
    NES_DEBUG("Create add scan and emit phase");
    return AddScanAndEmitPhase::create();
}
LowerPhysicalToGeneratableOperatorsPtr
DefaultPhaseFactory::createLowerPhysicalToGeneratableOperatorsPhase(QueryCompilerOptionsPtr) {
    NES_DEBUG("Create default lower pipeline plan phase");
    auto generatableOperatorProvider = DefaultGeneratableOperatorProvider::create();
    return LowerPhysicalToGeneratableOperators::create(generatableOperatorProvider);
}
CodeGenerationPhasePtr DefaultPhaseFactory::createCodeGenerationPhase(QueryCompilerOptionsPtr,
                                                                      Compiler::JITCompilerPtr jitCompiler) {
    NES_DEBUG("Create default code generation phase");
    // TODO create a option to choose between different code generators.
    auto codeGenerator = CCodeGenerator::create();
    return CodeGenerationPhase::create(codeGenerator, jitCompiler);
}
LowerToExecutableQueryPlanPhasePtr DefaultPhaseFactory::createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create lower to executable query plan phase");
    auto sourceProvider = DataSourceProvider::create(options);
    auto sinkProvider = DataSinkProvider::create();
    return LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
}
BufferOptimizationPhasePtr DefaultPhaseFactory::createBufferOptimizationPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create buffer optimization phase");
    return BufferOptimizationPhase::create(options->getOutputBufferOptimizationLevel());
}
PredicationOptimizationPhasePtr DefaultPhaseFactory::createPredicationOptimizationPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create predication optimization phase");
    return PredicationOptimizationPhase::create(options->isPredicationEnabled());
}
}// namespace NES::QueryCompilation::Phases