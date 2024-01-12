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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::Phases {

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
    * @brief Creates lower operator plan to executable query plan phase
    * @param QueryCompilerOptionsPtr options
    * @return LowerToExecutableQueryPlanPhasePtr
    */
    virtual LowerToExecutableQueryPlanPhasePtr createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options,
                                                                                     bool sourceSharing) = 0;

    /**
    * @brief Creates buffer optimization phase
    * @param QueryCompilerOptionsPtr options
    * @return BufferOptimizationPhasePtr
    */
    virtual BufferOptimizationPhasePtr createBufferOptimizationPhase(QueryCompilerOptionsPtr options) = 0;
};

}// namespace NES::QueryCompilation::Phases

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
