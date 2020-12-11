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

#ifndef NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#define NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#include <memory>

namespace NES::Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;

class JoinActionDescriptor;
typedef std::shared_ptr<JoinActionDescriptor> JoinActionDescriptorPtr;

class AbstractJoinHandler;
typedef std::shared_ptr<AbstractJoinHandler> AbstractJoinHandlerPtr;

template<class KeyType, class InputTypeLeft, class InputTypeRight>
class ExecutableNestedLoopJoinTriggerAction;
template<class KeyType, class InputTypeLeft, class InputTypeRight>
using ExecutableNestedLoopJoinTriggerActionPtr = std::shared_ptr<ExecutableNestedLoopJoinTriggerAction<KeyType, InputTypeLeft, InputTypeRight>>;

template<class KeyType, class InputTypeLeft, class InputTypeRight>
class BaseExecutableJoinAction;
template<class KeyType, class InputTypeLeft, class InputTypeRight>
using BaseExecutableJoinActionPtr = std::shared_ptr<BaseExecutableJoinAction<KeyType, InputTypeLeft, InputTypeRight>>;

class BaseJoinActionDescriptor;
typedef std::shared_ptr<BaseJoinActionDescriptor> BaseJoinActionDescriptorPtr;

class JoinOperatorHandler;
typedef std::shared_ptr<JoinOperatorHandler> JoinOperatorHandlerPtr;

}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
