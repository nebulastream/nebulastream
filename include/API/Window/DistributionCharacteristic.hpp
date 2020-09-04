#ifndef NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#define NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#include <memory>

namespace NES {

class DistributionCharacteristic;
typedef std::shared_ptr<DistributionCharacteristic> DistributionCharacteristicPtr;

/**
 * @brief The time stamp characteristic represents if an window is in event or processing time.
 */
class DistributionCharacteristic {
  public:
    /**
     * @brief The type as enum.
     */
    enum Type {
        Centralized,
        Distributed
    };
    explicit DistributionCharacteristic(Type type);

    /**
     * @brief Factory to create a distributed window type
     * @return DistributionCharacteristicPtr
     */
    static DistributionCharacteristicPtr createCentralizedWindowType();

    /**
     * @brief Factory to create a distributed window type
     * @return
     */
    static DistributionCharacteristicPtr createDistributedWindowType();

    /**
     * @return The DistributionCharacteristic type.
     */
    Type getType();


  private:
    Type type;
};

}// namespace NES
#endif//NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
