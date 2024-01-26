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
#ifdef UNIKERNEL_EXPORT
#include <OperatorHandlerTracer.hpp>
#endif
#include <Exceptions/RuntimeException.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution {

/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler
#ifndef UNIKERNEL_LIB
    : public Reconfigurable
#endif
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
     * @brief Default deconstructor
     */
#ifndef UNIKERNEL_LIB
    ~OperatorHandler() override = default;
#else
    virtual ~OperatorHandler() = default;
#endif
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

#ifndef UNIKERNEL_LIB
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
#endif
#ifdef UNIKERNEL_EXPORT
    NES::Runtime::Unikernel::OperatorHandlerDescriptor getDescriptor() const { return descriptor.value(); }

  protected:
    std::optional<NES::Runtime::Unikernel::OperatorHandlerDescriptor> descriptor;
    template<typename... Args>
    void trace(std::string&& className, std::string&& headerPath, size_t classSize, size_t alignment, Args&&... args) {
        this->descriptor.emplace(className,
                                 headerPath,
                                 classSize,
                                 alignment,
                                 std::vector<Unikernel::OperatorHandlerParameterDescriptor>{
                                     Unikernel::OperatorHandlerParameterDescriptor::of(std::forward<Args>(args))...});
    }
#endif
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
