#include <WaterMark/ProcessingTimeWaterMark.hpp>
#include <Util/Logger.hpp>
#include <chrono>
namespace NES {

ProcessingTimeWaterMark::ProcessingTimeWaterMark() {
}

uint64_t ProcessingTimeWaterMark::getWaterMark()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}


}// namespace NES