#ifndef NES_PROCESSINGTIMEWATERMARKSTRATEGY_HPP
#define NES_PROCESSINGTIMEWATERMARKSTRATEGY_HPP

#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES::Windowing {

class ProcessingTimeWatermarkStrategy;
typedef std::shared_ptr<ProcessingTimeWatermarkStrategy> ProcessingTimeWatermarkStrategyPtr;

class ProcessingTimeWatermarkStrategy : public WatermarkStrategy {
  public:
    ProcessingTimeWatermarkStrategy();

    static ProcessingTimeWatermarkStrategyPtr create();

    Type getType() override;
};

} // namespace namespace NES::Windowing

#endif//NES_PROCESSINGTIMEWATERMARKSTRATEGY_HPP
