#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>

namespace NES::Join {

BaseJoinActionDescriptorPtr LazyNestLoopJoinTriggerActionDescriptor::create() {
    return std::make_shared<LazyNestLoopJoinTriggerActionDescriptor>(LazyNestLoopJoinTriggerActionDescriptor());
}

JoinActionType LazyNestLoopJoinTriggerActionDescriptor::getActionType() {
    return this->action;
}

LazyNestLoopJoinTriggerActionDescriptor::LazyNestLoopJoinTriggerActionDescriptor()
    : BaseJoinActionDescriptor(LazyNestedLoopJoin) {
}

}// namespace NES::Windowing