#ifndef NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#include <Windowing/Watermark/Watermark.hpp>
#include <cstdint>
#include <memory>

namespace NES::Windowing {

class ProcessingTimeWatermarkGenerator : public Watermark {
  public:
    explicit ProcessingTimeWatermarkGenerator();

    /**
    * @brief this function returns the watermark value as a processing time value
    * @return watermark value
    */
    uint64_t getWatermark();
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
