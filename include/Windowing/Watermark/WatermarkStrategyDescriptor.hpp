#ifndef NES_WATERMARKSTRATEGYDESCRIPTOR_HPP
#define NES_WATERMARKSTRATEGYDESCRIPTOR_HPP

#include <Util/Logger.hpp>
#include <memory>

namespace NES::Windowing {

class WatermarkStrategyDescriptor;
typedef std::shared_ptr<WatermarkStrategyDescriptor> WatermarkStrategyDescriptorPtr;

class WatermarkStrategyDescriptor : public std::enable_shared_from_this<WatermarkStrategyDescriptor> {
  public:
    WatermarkStrategyDescriptor();
    virtual ~WatermarkStrategyDescriptor() = default;
    virtual bool equal(WatermarkStrategyDescriptorPtr other) = 0;

    /**
    * @brief Checks if the current node is of type WatermarkStrategyDescriptor
    * @tparam WatermarkStrategyType
    * @return bool true if node is of WatermarkStrategyDescriptor
    */
    template<class WatermarkStrategyType>
    bool instanceOf() {
        if (dynamic_cast<WatermarkStrategyType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the watermark strategy to a WatermarkStrategyType
    * @tparam WatermarkStrategyType
    * @return returns a shared pointer of the WatermarkStrategyType
    */
    template<class WatermarkStrategyType>
    std::shared_ptr<WatermarkStrategyType> as() {
        if (instanceOf<WatermarkStrategyType>()) {
            return std::dynamic_pointer_cast<WatermarkStrategyType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }
};
}// namespace NES::Windowing

#endif//NES_WATERMARKSTRATEGYDESCRIPTOR_HPP
