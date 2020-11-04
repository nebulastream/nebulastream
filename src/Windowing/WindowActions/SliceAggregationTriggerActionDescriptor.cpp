#include <Windowing/WindowActions/SliceAggregationTriggerActionDescriptor.hpp>

namespace NES::Windowing {

BaseWindowActionDescriptorPtr SliceAggregationTriggerActionDescriptor::create() {
    return std::make_shared<SliceAggregationTriggerActionDescriptor>(SliceAggregationTriggerActionDescriptor());
}

ActionType SliceAggregationTriggerActionDescriptor::getActionType() {
    return this->action;
}

SliceAggregationTriggerActionDescriptor::SliceAggregationTriggerActionDescriptor()
    : BaseWindowActionDescriptor(SliceAggregationTriggerAction) {
}

}// namespace NES::Windowing