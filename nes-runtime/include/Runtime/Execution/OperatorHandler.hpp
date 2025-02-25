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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
#include <Exceptions/RuntimeException.hpp>
#include <Runtime/Execution/MigratableStateInterface.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <list>

namespace NES::Runtime::Execution {

/**
 * StreamJoinOperatorHandler (and others) will temporarily contain code that handles sharing with different approaches to test for a
 * master thesis. Task #5113 describes different approaches. While under testing I will try to keep the code clean by naming methods
 * according to their approach and storing the helper enum class to know which approach is being tested. However some parts will
 * need to be commented out and adjusted to test each approach individually
 */
enum class SharedJoinApproach : uint8_t { UNSHARED, APPROACH_PROBING, APPROACH_DELETING, APPROACH_TOMBSTONE };

/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler : public virtual Reconfigurable, public virtual MigratableStateInterface {
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

    virtual void recreate();

    virtual bool shouldRecreate();

    /**
     * @brief Gets the state
     * @param startTS
     * @param stopTS
     * @return list of TupleBuffers
     */
    std::vector<Runtime::TupleBuffer> getStateToMigrate(uint64_t, uint64_t) override;

    std::vector<Runtime::TupleBuffer> serializeOperatorHandlerForMigration() override;

    virtual std::vector<Runtime::TupleBuffer> getSerializedPortion(uint64_t);


    /**
     * @brief Merges migrated slices
     * @param buffer
     */
    void restoreState(std::vector<Runtime::TupleBuffer>&) override;

    /**
     * @brief Restores state from the file
     * @param stream with the state data
     */
    void restoreStateFromFile(std::ifstream&) override;

    /**
     * @brief Default deconstructor
     */
    virtual ~OperatorHandler() override = default;

    /**
     * @brief Checks if the current operator handler is of type OperatorHandlerType
     * @tparam OperatorHandlerType
     * @return bool true if node is of OperatorHandlerType
     */
    template<class OperatorHandlerType>
    bool instanceOf() {
        if (dynamic_cast<OperatorHandlerType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a OperatorHandlerType
    * @tparam OperatorHandlerType
    * @return returns a shared pointer of the OperatorHandlerType
    */
    template<class OperatorHandlerType>
    std::shared_ptr<OperatorHandlerType> as() {
        if (instanceOf<OperatorHandlerType>()) {
            return std::dynamic_pointer_cast<OperatorHandlerType>(this->shared_from_this());
        }
        throw Exceptions::RuntimeException("OperatorHandler:: we performed an invalid cast of operator to type "
                                           + std::string(typeid(OperatorHandlerType).name()));
    }
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
