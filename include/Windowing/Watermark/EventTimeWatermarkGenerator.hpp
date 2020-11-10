#ifndef NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#include <Windowing/Watermark/Watermark.hpp>
#include <Windowing/Watermark/WatermarkStrategy.hpp>
#include <cstdint>
#include <memory>

namespace NES::Windowing {

class EventTimeWatermarkGenerator : public Watermark {
  public:
    explicit EventTimeWatermarkGenerator();

    /**
    * @brief this function returns the watermark value as a event time value
    * @return watermark value
    */
    uint64_t getWatermark();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
