#ifndef NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#include <memory>
#include <cstdint>
#include <WaterMark/WaterMark.hpp>

namespace NES {

class EventTimeWaterMark : public WaterMark {
  public:
    explicit EventTimeWaterMark();

    uint64_t getWaterMark();
};
}
#endif//NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
