#include <Windowing/WindowActions/WindowAggregationTriggerActionDescriptor.hpp>

namespace NES::Windowing {

BaseWindowActionDescriptorPtr WindowAggregationTriggerActionDescriptor::create() {
    return std::make_shared<WindowAggregationTriggerActionDescriptor>(WindowAggregationTriggerActionDescriptor());
}

ActionType WindowAggregationTriggerActionDescriptor::getActionType() {
    return this->action;
}

WindowAggregationTriggerActionDescriptor::WindowAggregationTriggerActionDescriptor()
    : BaseWindowActionDescriptor(WindowAggregationTriggerAction) {
}

}// namespace NES::Windowing