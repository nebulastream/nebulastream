#ifndef NES_EVENTTIMEWATERMARKSTRATEGY_HPP
#define NES_EVENTTIMEWATERMARKSTRATEGY_HPP

#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES::Windowing {

class EventTimeWatermarkStrategy;
typedef std::shared_ptr<EventTimeWatermarkStrategy> EventTimeWatermarkStrategyPtr;

class EventTimeWatermarkStrategy : public WatermarkStrategy {
  public:
    EventTimeWatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t delay);

    FieldAccessExpressionNodePtr getField();

    uint64_t getDelay();

    Type getType() override;

    static EventTimeWatermarkStrategyPtr create(FieldAccessExpressionNodePtr onField, uint64_t delay);
  private:
    // Field where the watermark should be retrieved
    FieldAccessExpressionNodePtr onField;

    // Watermark dela
    uint64_t delay;

};


} // namespace namespace NES::Windowing


#endif//NES_EVENTTIMEWATERMARKSTRATEGY_HPP
