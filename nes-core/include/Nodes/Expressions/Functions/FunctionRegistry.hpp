#ifndef NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_
#define NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_

#include <Util/PluginRegistry.hpp>
namespace NES {

class DataType;
class LogicalFunction;
using DataTypePtr = std::shared_ptr<DataType>;

using FunctionRegistry = Util::PluginFactory<LogicalFunction>;

class LogicalFunction {
  public:
    LogicalFunction() = default;
    [[nodiscard]] virtual DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const = 0;
    virtual ~LogicalFunction() = default;
};

class UnaryLogicalFunction : public LogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const final;
    [[nodiscard]] virtual DataTypePtr inferUnary(const DataTypePtr& input) const = 0;
};

}// namespace NES
#endif//NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_
