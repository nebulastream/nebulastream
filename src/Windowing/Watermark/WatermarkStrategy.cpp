#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES::Windowing {

WatermarkStrategy::WatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t delay) : onField(onField), delay(delay) {

}
WatermarkStrategyPtr WatermarkStrategy::create(FieldAccessExpressionNodePtr onField, uint64_t delay) {
    return std::make_shared<NES::Windowing::WatermarkStrategy>(onField, delay);
}
FieldAccessExpressionNodePtr WatermarkStrategy::getField() {
    return onField;
}
uint64_t WatermarkStrategy::getDelay() {
    return delay;
}
bool WatermarkStrategy::equal(WatermarkStrategyPtr other) {
    return (delay == other->getDelay() && onField == other->getField());
}

} // namespace NES::Windowing