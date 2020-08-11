#ifndef NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
#include <Windows/Watermark/Watermark.hpp>
#include <cstdint>
#include <memory>

namespace NES{

class ProcessingTimeWatermark : public Watermark {
  public:
    explicit ProcessingTimeWatermark();

    /**
    * @brief this function returns the watermark value as a processing time value
    * @return watermark value
    */
    uint64_t getWatermark();
};

}
#endif//NES_INCLUDE_WATERMARK_PROCESSINGTIMEWATERMARK_HPP_
