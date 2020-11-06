#ifndef NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP
#define NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP

#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

class ProcessingTimeWatermarkStrategyDescriptor;
typedef std::shared_ptr<ProcessingTimeWatermarkStrategyDescriptor> ProcessingTimeWatermarkStrategyDescriptorPtr;

class ProcessingTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create();

  private:
    explicit ProcessingTimeWatermarkStrategyDescriptor();
};

} // namespace NES::Windowing



#endif//NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP
