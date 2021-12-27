#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

class PhysicalKeyedThreadLocalPreAggregationOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    PhysicalKeyedThreadLocalPreAggregationOperator(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler);

    static std::shared_ptr<PhysicalOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler) {
        return std::make_shared<PhysicalKeyedThreadLocalPreAggregationOperator>(Util::getNextOperatorId(),
                                                                                inputSchema,
                                                                                outputSchema,
                                                                                keyedEventTimeWindowHandler);
    }

    std::string toString() const override;
    OperatorNodePtr copy() override;

    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> getWindowHandler() {
        return keyedEventTimeWindowHandler;
    }

  private:
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler;
    SchemaPtr inputSchema;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
