#include <Util/Logger.hpp>
#include <Windowing/Watermark/ProcessingTimeWatermarkGenerator.hpp>
#include <chrono>
namespace NES {

ProcessingTimeWatermarkGenerator::ProcessingTimeWatermarkGenerator() {
}

uint64_t ProcessingTimeWatermarkGenerator::getWatermark() {
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_DEBUG("ProcessingTimeWatermarkGenerator::getWatermark generate ts=" << ts);
    return ts;
}

}// namespace NES