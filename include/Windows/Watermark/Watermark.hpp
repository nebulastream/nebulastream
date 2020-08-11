#ifndef NES_INCLUDE_WATERMARK_WATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_WATERMARK_HPP_

#include <memory>
#include <cstdint>
namespace NES{

class Watermark {
  public:
    explicit Watermark();

    /**
     * @brief this function returns the watermark value depending of the type of the inherited class
     * @return watermark value
     */
    virtual uint64_t getWatermark() = 0;
};

typedef std::shared_ptr<Watermark> WatermarkPtr;
}
#endif//NES_INCLUDE_WATERMARK_WATERMARK_HPP_






