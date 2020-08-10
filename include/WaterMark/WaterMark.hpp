#ifndef NES_INCLUDE_WATERMARK_WATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_WATERMARK_HPP_

#include <memory>
#include <cstdint>
namespace NES{

class WaterMark {
  public:
    explicit WaterMark();

    virtual uint64_t getWaterMark() = 0;
};

typedef std::shared_ptr<WaterMark> WaterMarkPtr;
}
#endif//NES_INCLUDE_WATERMARK_WATERMARK_HPP_






