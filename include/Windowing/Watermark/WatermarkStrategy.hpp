#ifndef NES_WATERMARKSTRATEGY_HPP
#define NES_WATERMARKSTRATEGY_HPP

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class WatermarkStrategy : public std::enable_shared_from_this<WatermarkStrategy> {
  public:
    WatermarkStrategy();

    enum Type {
        EventTimeWatermark,
        ProcessingTimeWatermark,
    };

    virtual Type getType() = 0;

    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(shared_from_this());
    }
};
}// namespace NES::Windowing
#endif//NES_WATERMARKSTRATEGY_HPP
