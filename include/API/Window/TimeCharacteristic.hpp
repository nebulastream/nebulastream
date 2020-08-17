#ifndef NES_INCLUDE_API_WINDOW_TIMECHARACTERISTIC_HPP_
#define NES_INCLUDE_API_WINDOW_TIMECHARACTERISTIC_HPP_
#include <memory>

namespace NES {

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class TimeCharacteristic;
typedef std::shared_ptr<TimeCharacteristic> TimeCharacteristicPtr;

class ExpressionItem;

/**
 * @brief The time stamp characteristic represents if an window is in event or processing time.
 */
class TimeCharacteristic {
  public:
    /**
     * @brief The type as enum.
     */
    enum Type {
        ProcessingTime,
        EventTime
    };
    explicit TimeCharacteristic(Type type);
    TimeCharacteristic(Type type, AttributeFieldPtr field);

    /**
     * @brief Factory to create a time characteristic for processing time window
     * @return TimeCharacteristicPtr
     */
    static TimeCharacteristicPtr createProcessingTime();

    /**
     * @brief Factory to create a event time window with an time extractor on a specific field.
     * @param field the field from which we want to extract the time.
     * @return
     */
    static TimeCharacteristicPtr createEventTime(ExpressionItem field);

    /**
     * @return The TimeCharacteristic type.
     */
    Type getType();

    /**
     * @return  If it is a event time window this returns the field, from which we extract the time stamp.
     */
    AttributeFieldPtr getField();

  private:
    Type type;
    AttributeFieldPtr field;
};

}// namespace NES
#endif//NES_INCLUDE_API_WINDOW_TIMECHARACTERISTIC_HPP_
