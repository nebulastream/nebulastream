#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(ExpressionItem onField, TimeMeasure delay) : onField(onField), delay(delay) {
}

WatermarkStrategyDescriptorPtr EventTimeWatermarkStrategyDescriptor::create(ExpressionItem onField, TimeMeasure delay) {
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(Windowing::EventTimeWatermarkStrategyDescriptor(onField, delay));
}
ExpressionItem EventTimeWatermarkStrategyDescriptor::getOnField() {
    return onField;
}
TimeMeasure EventTimeWatermarkStrategyDescriptor::getDelay() {
    return delay;
}
bool EventTimeWatermarkStrategyDescriptor::equal(WatermarkStrategyDescriptorPtr other) {
    auto eventTimeWatermarkStrategyDescriptor = other->as<EventTimeWatermarkStrategyDescriptor>();
    return eventTimeWatermarkStrategyDescriptor->onField.getExpressionNode() == onField.getExpressionNode() && eventTimeWatermarkStrategyDescriptor->delay.getTime() == delay.getTime();
}
}// namespace NES::Windowing