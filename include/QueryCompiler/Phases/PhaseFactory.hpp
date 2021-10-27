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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_PHASE_FACTORY_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_PHASE_FACTORY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
namespace Phases {

/**
 * @brief An abstract factory, which allows the query compiler to create instances of particular phases,
 * without knowledge about the concrete implementations. This ensures extendability.
 */
class PhaseFactory {
  public:
    /**
     * @brief Creates a lower logical operator to physical operator phase
     * @param QueryCompilerOptionsPtr options
     * @return LowerLogicalToPhysicalOperatorsPtr
     */
    virtual LowerLogicalToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) = 0;

    /**
     * @brief Creates pipelining phase
     * @param QueryCompilerOptionsPtr options
     * @return PipeliningPhasePtr
     */
    virtual PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) = 0;

    /**
    * @brief Creates add scan and emit phase
    * @param QueryCompilerOptionsPtr options
    * @return AddScanAndEmitPhasePtr
    */
    virtual AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) = 0;

    /**
    * @brief Creates lower physical operator to generatable operator phase
    * @param QueryCompilerOptionsPtr options
    * @return LowerPhysicalToGeneratableOperatorsPtr
    */
    virtual LowerPhysicalToGeneratableOperatorsPtr
    createLowerPhysicalToGeneratableOperatorsPhase(QueryCompilerOptionsPtr options) = 0;

    /**
    * @brief Creates code generation phase
    * @param QueryCompilerOptionsPtr options
    * @return CodeGenerationPhasePtr
    */
    virtual CodeGenerationPhasePtr createCodeGenerationPhase(QueryCompilerOptionsPtr options,
                                                             Compiler::JITCompilerPtr jitCompiler) = 0;

    /**
    * @brief Creates lower operator plan to executable query plan phase
    * @param QueryCompilerOptionsPtr options
    * @return LowerToExecutableQueryPlanPhasePtr
    */
    virtual LowerToExecutableQueryPlanPhasePtr createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options) = 0;

    /**
    * @brief Creates buffer optimization phase
    * @param QueryCompilerOptionsPtr options
    * @return BufferOptimizationPhasePtr
    */
    virtual BufferOptimizationPhasePtr createBufferOptimizationPhase(QueryCompilerOptionsPtr options) = 0;

    /**
    * @brief Creates Predication optimization phase
    * @param QueryCompilerOptionsPtr options
    * @return PredicationOptimizationPhasePtr
    */
    virtual PredicationOptimizationPhasePtr createPredicationOptimizationPhase(QueryCompilerOptionsPtr options) = 0;
};

}// namespace Phases
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_PHASES_PHASE_FACTORY_HPP_
