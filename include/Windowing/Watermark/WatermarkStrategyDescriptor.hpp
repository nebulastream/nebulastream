#ifndef NES_WATERMARKSTRATEGYDESCRIPTOR_HPP
#define NES_WATERMARKSTRATEGYDESCRIPTOR_HPP

#include <memory>
namespace NES::Windowing {

class WatermarkStrategyDescriptor : public std::enable_shared_from_this<WatermarkStrategyDescriptor> {
  public:
    WatermarkStrategyDescriptor();
    virtual ~WatermarkStrategyDescriptor() = default;
};

typedef std::shared_ptr<WatermarkStrategyDescriptor> WatermarkStrategyDescriptorPtr;

}

#endif//NES_WATERMARKSTRATEGYDESCRIPTOR_HPP
