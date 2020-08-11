#include <Windows/Watermark/ProcessingTimeWatermark.hpp>
#include <Util/Logger.hpp>
#include <chrono>
namespace NES {

ProcessingTimeWatermark::ProcessingTimeWatermark() {
}

uint64_t ProcessingTimeWatermark::getWatermark()
{
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_DEBUG("ProcessingTimeWatermark::getWatermark generate ts=" << ts);
    return ts;
}


}// namespace NES