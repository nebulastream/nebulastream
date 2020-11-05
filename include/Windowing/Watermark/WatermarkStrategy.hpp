#ifndef NES_WATERMARKSTRATEGY_HPP
#define NES_WATERMARKSTRATEGY_HPP

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class WatermarkStrategy {
  public:
    WatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t delay);
    static WatermarkStrategyPtr create(FieldAccessExpressionNodePtr onField, uint64_t delay);

    FieldAccessExpressionNodePtr getField();
    uint64_t getDelay();

    bool equal(WatermarkStrategyPtr other);
  private:
    // Field where the watermark should be retrieved
    FieldAccessExpressionNodePtr onField;

    // Watermark dela
    uint64_t delay;
};
}
#endif//NES_WATERMARKSTRATEGY_HPP
