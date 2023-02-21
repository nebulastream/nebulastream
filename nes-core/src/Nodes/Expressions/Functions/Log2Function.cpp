#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/Functions/FunctionRegistry.hpp>

namespace NES {

/*
 * Defines the log2 function and registers it to the FunctionRegistry.
 */
class Log2Function : public UnaryLogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferUnary(const DataTypePtr& input) const override {
        if (input->isNumeric()) {
            NES_ERROR("LogExpressions can only be evaluated on numeric values.");
        }
        // Output values can become highly negative for inputs close to +0. Set Double as output stamp.
        return DataTypeFactory::createDouble();
    }
};

[[maybe_unused]] static FunctionRegistry::Add<Log2Function> logFunction("log2");

}// namespace NES