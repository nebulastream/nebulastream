#include <Util/Logger.hpp>
#include <Windows/Watermark/EventTimeWatermarkGenerator.hpp>
namespace NES {

EventTimeWatermarkGenerator::EventTimeWatermarkGenerator() {
}

uint64_t EventTimeWatermarkGenerator::getWatermark() {
    NES_NOT_IMPLEMENTED();
    return 0;
}

}// namespace NES