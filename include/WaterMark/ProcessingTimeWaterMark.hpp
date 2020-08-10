#ifndef NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#include <memory>
#include <cstdint>
#include <WaterMark/WaterMark.hpp>

namespace NES{

class ProcessingTimeWaterMark : public WaterMark {
  public:
    explicit ProcessingTimeWaterMark();

    uint64_t getWaterMark();
};

}
#endif//NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
