#include <Util/Logger.hpp>
#include <Windows/Watermark/EventTimeWatermark.hpp>
namespace NES {

EventTimeWatermark::EventTimeWatermark() {
}

uint64_t EventTimeWatermark::getWatermark() {
    NES_NOT_IMPLEMENTED();
    return 0;
}

}// namespace NES