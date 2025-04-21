/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <DataType.hpp>
#include <generator.hpp>

namespace NES
{

class ExpressionValue;
/**
 * @brief this indicates an function, which is a parameter for a FilterOperator or a MapOperator.
 * Each function declares a stamp, which expresses the data type of this function.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class Expression
{
public:
    virtual ~Expression() = default;

    ///Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema, std::vector<ExpressionValue>& children) = 0;
    std::optional<DataType> getStamp() const { return stamp; }

    /// Create a deep copy of this function node.
    [[nodiscard]] virtual std::shared_ptr<Expression> clone() const = 0;

    bool virtual equals(const Expression&) const = 0;

    virtual std::string toString() const = 0;

protected:
    std::optional<DataType> stamp;

    friend class FunctionSerializationUtil;
};

class ExpressionValue
{
    COW<Expression> expression;
    std::vector<ExpressionValue> children;

public:
    void inferStamp(const Schema& schema);
    std::optional<DataType> getStamp() const;

    template <std::derived_from<Expression> ExpressionT>
    ExpressionT* as()
    {
        return dynamic_cast<ExpressionT*>(expression.operator->());
    }

    template <std::derived_from<Expression> ExpressionT>
    const ExpressionT* as() const
    {
        return dynamic_cast<const ExpressionT*>(expression.operator->());
    }

    bool operator==(ExpressionValue other) const;
    const std::vector<ExpressionValue>& getChildren() const;

    ExpressionValue(COW<Expression> expression, std::vector<ExpressionValue> children);

    coro::generator<const ExpressionValue&> dfs() const;

    friend auto format_as(const ExpressionValue& value) -> std::string;
};

auto format_as(const ExpressionValue& value) -> std::string;

template <typename T, typename... Args>
requires(std::derived_from<T, Expression> && std::constructible_from<T, Args...> && (!std::same_as<Args, ExpressionValue> && ...))
ExpressionValue make_expression(Args&&... args)
{
    return ExpressionValue(COW<Expression>(std::make_shared<T>(std::forward<Args>(args)...)), {});
}

template <typename T, typename... Args>
requires(std::derived_from<T, Expression> && std::constructible_from<T, Args...> && (!std::same_as<Args, ExpressionValue> && ...))
ExpressionValue make_expression(std::initializer_list<ExpressionValue> children, Args&&... args)
{
    return ExpressionValue(COW<Expression>(std::make_shared<T>(std::forward<Args>(args)...)), std::move(children));
}

template <typename T, typename... Args>
requires(std::derived_from<T, Expression> && std::constructible_from<T, Args...> && (!std::same_as<Args, ExpressionValue> && ...))
ExpressionValue make_expression(std::vector<ExpressionValue> children, Args&&... args)
{
    return ExpressionValue(COW<Expression>(std::make_shared<T>(std::forward<Args>(args)...)), std::move(children));
}
}
