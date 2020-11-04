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
  private:
    FieldAccessExpressionNodePtr onField;
    uint64_t delay;
};
}
#endif//NES_WATERMARKSTRATEGY_HPP
