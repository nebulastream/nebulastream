//
// Created by ls on 4/18/25.
//

#include <memory>
#include <ranges>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DataType.hpp>
#include "Util/Logger/LogLevel.hpp"

namespace NES
{
class DataTypeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("VariableSizedDataTest.log", LogLevel::LOG_DEBUG); }
};

struct Schema
{
    using FieldIdentifier = std::string;
    std::unordered_map<FieldIdentifier, DataType> fields;
};

struct Expression
{
    virtual ~Expression() = default;
    virtual DataType getType(const Schema& schema) = 0;
};

struct FunctionExpression : Expression
{
    FunctionExpression(std::vector<std::unique_ptr<Expression>> arguments, std::string name) : arguments(std::move(arguments)), name(name)
    {
    }
    std::vector<std::unique_ptr<Expression>> arguments;
    std::string name;

    DataType getType(const Schema& schema) override
    {
        std::vector<DataType> types
            = arguments | std::views::transform([&](const auto& args) { return args->getType(schema); }) | std::ranges::to<std::vector>();
        if (auto function = FunctionRegistry::instance().lookup(name, types))
        {
            return function->returnType;
        }
        throw std::runtime_error(fmt::format("Function `{}({})` does not exist", name, fmt::join(types, ", ")));
    }
};

struct ConstantExpression : Expression
{
    explicit ConstantExpression(std::string constant_value) : constantValue(std::move(constant_value)) { }
    std::string constantValue;
    DataType getType(const Schema&) override { return NES::constant(constantValue); }
};

struct NameExpression : Expression
{
    explicit NameExpression(std::string name) : name(std::move(name)) { }
    std::string name;
    DataType getType(const Schema& schema) override
    {
        if (auto it = schema.fields.find(name); it != schema.fields.end())
        {
            return it->second;
        }
        throw std::runtime_error(fmt::format("Unknown name `{}`", name));
    }
};

template <typename... Args>
std::unique_ptr<Expression> function(std::string name, Args&&... args)
{
    return std::make_unique<FunctionExpression>(make_vector<std::unique_ptr<Expression>>(std::forward<Args>(args)...), std::move(name));
}

template <typename... Args>
std::unique_ptr<Expression> constantExpression(std::string value)
{
    return std::make_unique<ConstantExpression>(value);
}

template <typename... Args>
std::unique_ptr<Expression> nameExpression(std::string name)
{
    return std::make_unique<NameExpression>(name);
}


TEST_F(DataTypeTest, BasicTest)
{
    Schema schema;
    schema.fields.try_emplace("p1", DataTypeRegistry::instance().lookup("Point").value());
    schema.fields.try_emplace("p2", DataTypeRegistry::instance().lookup("Point").value());
    schema.fields.try_emplace("p3", DataTypeRegistry::instance().lookup("Point").value().asNullable());


    function("NEGATE", function("Integer", constantExpression("32")))->getType(schema);
    function(
        "x",
        function(
            "ADD",
            function(
                "Point",
                function("Integer", constantExpression("2")),
                function("Integer", constantExpression("2")),
                function("Integer", constantExpression("2"))),
            nameExpression("p1")))
        ->getType(schema);

    function("IS_NULL", nameExpression("p3"))->getType(schema);

    function(
        "GET",
        function("LIST", function("NULLABLE", nameExpression("p1")), nameExpression("p3")),
        function("Integer", constantExpression("1")))
        ->getType(schema);
}
}