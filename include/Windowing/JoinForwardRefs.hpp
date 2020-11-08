#ifndef NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#define NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#include <memory>

namespace NES::Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;


class JoinActionDescriptor;
typedef std::shared_ptr<JoinActionDescriptor> JoinActionDescriptorPtr;

template<class KeyType>
class ExecutableNestedLoopJoinTriggerAction;
template<class KeyType>
using ExecutableNestedLoopJoinTriggerActionPtr = std::shared_ptr<ExecutableNestedLoopJoinTriggerAction<KeyType>>;


template<class KeyType>
class BaseExecutableJoinAction;
template<class KeyType>
using BaseExecutableJoinActionPtr = std::shared_ptr<BaseExecutableJoinAction<KeyType>>;


class BaseJoinActionDescriptor;
typedef std::shared_ptr<BaseJoinActionDescriptor> BaseJoinActionDescriptorPtr;

}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
