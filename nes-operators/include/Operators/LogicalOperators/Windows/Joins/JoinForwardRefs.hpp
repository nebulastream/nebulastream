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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
#include <memory>

namespace NES {

class FieldAccessExpressionNode;
using FieldAccessExpressionNodePtr = std::shared_ptr<FieldAccessExpressionNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Join {
class LogicalJoinDefinition;
using LogicalJoinDefinitionPtr = std::shared_ptr<LogicalJoinDefinition>;

namespace Experimental {
class LogicalBatchJoinDefinition;
using LogicalBatchJoinDefinitionPtr = std::shared_ptr<LogicalBatchJoinDefinition>;
}// namespace Experimental
class JoinActionDescriptor;
using JoinActionDescriptorPtr = std::shared_ptr<JoinActionDescriptor>;

class AbstractJoinHandler;
using AbstractJoinHandlerPtr = std::shared_ptr<AbstractJoinHandler>;

template<class KeyType, class InputTypeLeft, class InputTypeRight>
class ExecutableNestedLoopJoinTriggerAction;
template<class KeyType, class InputTypeLeft, class InputTypeRight>
using ExecutableNestedLoopJoinTriggerActionPtr =
    std::shared_ptr<ExecutableNestedLoopJoinTriggerAction<KeyType, InputTypeLeft, InputTypeRight>>;

template<class KeyType, class InputTypeLeft, class InputTypeRight>
class BaseExecutableJoinAction;
template<class KeyType, class InputTypeLeft, class InputTypeRight>
using BaseExecutableJoinActionPtr = std::shared_ptr<BaseExecutableJoinAction<KeyType, InputTypeLeft, InputTypeRight>>;

class BaseJoinActionDescriptor;
using BaseJoinActionDescriptorPtr = std::shared_ptr<BaseJoinActionDescriptor>;

class JoinOperatorHandler;
using JoinOperatorHandlerPtr = std::shared_ptr<JoinOperatorHandler>;

namespace Experimental {
class AbstractBatchJoinHandler;
using AbstractBatchJoinHandlerPtr = std::shared_ptr<AbstractBatchJoinHandler>;

class BatchJoinOperatorHandler;
using BatchJoinOperatorHandlerPtr = std::shared_ptr<BatchJoinOperatorHandler>;
}// namespace Experimental
}// namespace Join
}// namespace NES
#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
