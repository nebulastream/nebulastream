#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

EventTimeWatermarkStrategy::EventTimeWatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t delay) : onField(onField), delay(delay) {
}

FieldAccessExpressionNodePtr NES::Windowing::EventTimeWatermarkStrategy::getField() {
    return onField;
}
uint64_t EventTimeWatermarkStrategy::getDelay() {
    return delay;
}

WatermarkStrategy::Type EventTimeWatermarkStrategy::getType() {
    return WatermarkStrategy::EventTimeWatermark;
}
EventTimeWatermarkStrategyPtr EventTimeWatermarkStrategy::create(FieldAccessExpressionNodePtr onField, uint64_t delay) {
    return std::make_shared<Windowing::EventTimeWatermarkStrategy>(onField, delay);
}

} // namespace NES::Windowing

