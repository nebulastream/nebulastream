#ifndef NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#define NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#include <memory>

namespace NES::Windowing {

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
        Complete,
        Slicing,
        Combining
    };
    explicit DistributionCharacteristic(Type type);

    /**
     * @brief Factory to create central window that do slcicing and combining
     * @return DistributionCharacteristicPtr
     */
    static DistributionCharacteristicPtr createCompleteWindowType();

    /**
     * @brief Factory to to create a window slicer
     * @return
     */
    static DistributionCharacteristicPtr createSlicingWindowType();

    /**
    * @brief Factory to create a window combiner
    * @return
    */
    static DistributionCharacteristicPtr createCombiningWindowType();

    /**
     * @return The DistributionCharacteristic type.
     */
    Type getType();

  private:
    Type type;
};

}// namespace NES
#endif//NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
