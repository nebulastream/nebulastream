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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
namespace Phases {

/**
 * @brief A abstract factory, which allows the query compiler to create instances of particular phases,
 * without knowledge about the concrete implementations. This ensures extendability.
 */
class PhaseFactory {
  public:
    virtual const LowerLogicalToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const LowerPhysicalToGeneratableOperatorsPtr
    createLowerPhysicalToGeneratableOperatorsPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const CodeGenerationPhasePtr createCodeGenerationPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const LowerToExecutableQueryPlanPhasePtr createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options) = 0;
};

}// namespace Phases
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
