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
#include <Exceptions/RuntimeException.hpp>
#include <Runtime/Execution/MigratableStateInterface.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>

namespace NES::Runtime::Execution
{
/// Forward declaration of PipelineExecutionContext, which directly includes OperatorHandler
class PipelineExecutionContext;
using PipelineExecutionContextPtr = std::shared_ptr<PipelineExecutionContext>;
/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler : public virtual Reconfigurable, public virtual MigratableStateInterface
{
public:
    /**
     * @brief Default constructor
     */
    OperatorHandler() = default;

    /**
     * @brief Starts the operator handler.
     * @param pipelineExecutionContext
     * @param localStateVariableId
     * @param stateManager
     */
    virtual void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the operator handler.
     * @param pipelineExecutionContext
     */
    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    /**
     * @brief Gets the state
     * @param startTS
     * @param stopTS
     * @return list of TupleBuffers
     */
    std::vector<Memory::TupleBuffer> getStateToMigrate(uint64_t, uint64_t) override;

    /**
     * @brief Merges migrated slices
     * @param buffer
     */
    void restoreState(std::vector<Memory::TupleBuffer>&) override;

    /**
     * @brief Default deconstructor
     */
    virtual ~OperatorHandler() override = default;
};
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;
}
