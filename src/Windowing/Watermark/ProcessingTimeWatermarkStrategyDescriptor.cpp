#include <Windowing/Watermark/ProcessingTimeWatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

ProcessingTimeWatermarkStrategyDescriptor::ProcessingTimeWatermarkStrategyDescriptor() {
}

WatermarkStrategyDescriptorPtr ProcessingTimeWatermarkStrategyDescriptor::create() {
    return std::make_shared<ProcessingTimeWatermarkStrategyDescriptor>(Windowing::ProcessingTimeWatermarkStrategyDescriptor());
}

} // namespace NES::Windowing