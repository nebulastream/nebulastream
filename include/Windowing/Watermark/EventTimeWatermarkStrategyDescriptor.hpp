#ifndef NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP
#define NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP

#include <API/Expressions/Expressions.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES::Windowing {

class EventTimeWatermarkStrategyDescriptor;
typedef std::shared_ptr<EventTimeWatermarkStrategyDescriptor> EventTimeWatermarkStrategyDescriptorPtr;

class EventTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create(ExpressionItem onField, TimeMeasure delay);

    ExpressionItem getOnField();

    TimeMeasure getDelay();

    bool equal(WatermarkStrategyDescriptorPtr other) override;

  private:
    // Field where the watermark should be retrieved
    ExpressionItem onField;

    // watermark delay
    TimeMeasure delay;

    explicit EventTimeWatermarkStrategyDescriptor(ExpressionItem onField, TimeMeasure delay);
};

}// namespace NES::Windowing

#endif//NES_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP
