#ifndef NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
#include <Windows/Watermark/Watermark.hpp>
#include <cstdint>
#include <memory>

namespace NES {

class EventTimeWatermarkGenerator : public Watermark {
  public:
    explicit EventTimeWatermarkGenerator();

    /**
    * @brief this function returns the watermark value as a event time value
    * @return watermark value
    */
    uint64_t getWatermark();
};
}// namespace NES
#endif//NES_INCLUDE_WATERMARK_EVENTTIMEWATERMARK_HPP_
