#ifndef NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_
#define NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_

#include <Util/PluginRegistry.hpp>
namespace NES {
class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class LogicalFunction {
  public:
    LogicalFunction() = default;
    [[nodiscard]] virtual DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const = 0;
    virtual ~LogicalFunction() = default;
};

class LogicalFunctionProvider {
  public:
    LogicalFunctionProvider() = default;
    [[nodiscard]] virtual std::unique_ptr<LogicalFunction> create() const = 0;
    virtual ~LogicalFunctionProvider() = default;
};

class FunctionRegistry {
  private:
    using registry = Util::NamedPluginRegistry<LogicalFunctionProvider>;

    template<class T>
        requires std::is_class_v<T> && std::is_base_of_v<LogicalFunction, T>
    class TypedProvider : public LogicalFunctionProvider {
      public:
        [[nodiscard]] std::unique_ptr<LogicalFunction> create() const override { return std::make_unique<T>(); }
    };

  public:
    template<typename V>
    class Add {
      public:
        explicit Add(std::string name) { static auto type = registry::Add<TypedProvider<V>>(name); }
    };

    static std::unique_ptr<LogicalFunction> getFunction(const std::string& name) { return registry::getPlugin(name)->create(); }
};

class UnaryLogicalFunction : public LogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const final;
    [[nodiscard]] virtual DataTypePtr inferUnary(const DataTypePtr& input) const = 0;
};

}// namespace NES
#endif//NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_FUNCTIONREGISTRY_HPP_
