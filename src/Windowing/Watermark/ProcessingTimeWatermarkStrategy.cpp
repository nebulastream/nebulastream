#include <Windowing/Watermark/ProcessingTimeWatermarkStrategy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

ProcessingTimeWatermarkStrategy::ProcessingTimeWatermarkStrategy() {
}
ProcessingTimeWatermarkStrategyPtr ProcessingTimeWatermarkStrategy::create() {
    return std::make_shared<ProcessingTimeWatermarkStrategy>();
}
WatermarkStrategy::Type ProcessingTimeWatermarkStrategy::getType() {
    return WatermarkStrategy::ProcessingTimeWatermark;
}
} //namespace NES::Windowing