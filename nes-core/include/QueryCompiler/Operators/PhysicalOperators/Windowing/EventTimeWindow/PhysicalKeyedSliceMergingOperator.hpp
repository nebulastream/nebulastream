#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

class PhysicalKeyedSliceMergingOperator : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    PhysicalKeyedSliceMergingOperator(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler);

    static std::shared_ptr<PhysicalKeyedSliceMergingOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler) {
        return std::make_shared<PhysicalKeyedSliceMergingOperator>(Util::getNextOperatorId(),
                                                                   inputSchema,
                                                                   outputSchema,
                                                                   keyedEventTimeWindowHandler);
    }

    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> getWindowHandler() {
        return keyedEventTimeWindowHandler;
    }

    std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
