#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>

namespace NES::Windowing {

WindowActionDescriptorPtr CompleteAggregationTriggerActionDescriptor::create() {
    return std::make_shared<CompleteAggregationTriggerActionDescriptor>(CompleteAggregationTriggerActionDescriptor());
}

ActionType CompleteAggregationTriggerActionDescriptor::getActionType() {
    return this->action;
}

CompleteAggregationTriggerActionDescriptor::CompleteAggregationTriggerActionDescriptor()
    : BaseWindowActionDescriptor(WindowAggregationTriggerAction) {
}

}// namespace NES::Windowing