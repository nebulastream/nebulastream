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

#include <concepts>
#include <set>
#include <string>
#include <typeindex>
#include <typeinfo>

#include <Plans/LogicalPlan.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Functions/LogicalFunction.hpp>

/**
 * @file Rule.hpp
 * @brief Type-erased optimizer rule abstraction parameterized over a type.
 *
 * A Rule<U> is a value-semantic, type-erased wrapper around any type satisfying
 * RuleConcept<T, U>: a rewrite rule that transforms a unit of type U into another
 * unit of type U. Rules declare their dependencies via dependsOn(), enabling
 * the RuleManager to enforce application order.
 *
 * ## Key types
 * - RuleConcept<T, U>  — concept constraining a concrete rule type T for unit type U.
 * - Rule<U>            — type-erased handle; aliased from TypedRule<U>.
 * - TypedRule<U, C>    — full wrapper; C=ErasedRule<U> for the erased form,
 *                        C=ConcreteType for a statically-typed handle.
 * - ErasedRule<U>      — abstract base defining the virtual interface; lives in
 *                        detail:: and is never used directly by callers.
 */

namespace NES
{
namespace detail
{
template <typename U>
struct ErasedRule;
}

template <typename U, typename Checked = detail::ErasedRule<U>>
struct TypedRule;

template <typename U>
using Rule = TypedRule<U>;

template <typename T, typename U>
concept RuleConcept = requires(const T& thisRule, U unit, const T& rhs) {
    { thisRule.getType() } -> std::convertible_to<const std::type_info&>;
    { thisRule.getName() } -> std::convertible_to<std::string_view>;

    { thisRule.dependsOn() } -> std::convertible_to<std::set<std::type_index>>;

    { thisRule.apply(unit) } -> std::same_as<U>;

    { thisRule == rhs } -> std::convertible_to<bool>;
};

namespace detail
{
template<typename  U>
struct ErasedRule
{
    virtual ~ErasedRule() = default;

    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    [[nodiscard]] virtual std::string_view getName() const = 0;

    [[nodiscard]] virtual std::set<std::type_index> dependsOn() const = 0;

    [[nodiscard]] virtual U apply(U unit) const = 0;

    [[nodiscard]] virtual bool equals(const ErasedRule& other) const = 0;

    friend bool operator==(const ErasedRule& lhs, const ErasedRule& rhs) { return lhs.equals(rhs); }

private:
    template <typename, typename>
    friend struct NES::TypedRule;
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <typename U, RuleConcept<U> RuleType>
struct RuleModel;
}

template <typename U, typename Checked>
struct TypedRule
{
    template <RuleConcept<U> T>
    requires std::same_as<Checked, detail::ErasedRule<U>>
    TypedRule(TypedRule<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    template <typename T>
    TypedRule(const T& rule) : self(std::make_shared<detail::RuleModel<U, T>>(rule)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <RuleConcept<U> T>
    TypedRule(const detail::RuleModel<U,T>& rule) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<detail::RuleModel<U, T>>(rule.impl))
    {
    }

    explicit TypedRule(std::shared_ptr<const detail::ErasedRule<U>> rule) : self(std::move(rule)) { }

    TypedRule() = delete;

    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<Checked, detail::ErasedRule<U>>)
        {
            return *std::dynamic_pointer_cast<const detail::ErasedRule<U>>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<detail::RuleModel<U, Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const { return get(); }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<detail::ErasedRule<U>, Checked>)
        {
            return std::dynamic_pointer_cast<const detail::ErasedRule<U>>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const detail::RuleModel<U, Checked>>(self);
            return &casted->impl;
        }
    }

    template <RuleConcept<U> T>
    std::optional<TypedRule<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const detail::RuleModel<U, T>>(self))
        {
            return TypedRule<T>{std::static_pointer_cast<const detail::ErasedRule<U>>(model)};
        }
        return std::nullopt;
    }

    template <typename T>
    requires(!RuleConcept<T, U>)
    std::optional<std::shared_ptr<const Castable<T>>> tryGetAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        return std::nullopt;
    }

    template <RuleConcept<U> T>
    TypedRule<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const detail::RuleModel<U, T>>(self))
        {
            return TypedRule<T>{std::static_pointer_cast<const detail::ErasedRule<U>>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    template <typename T>
    requires(!RuleConcept<T, U>)
    std::shared_ptr<const Castable<T>> getAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
        std::unreachable();
    }

    [[nodiscard]] const std::type_info& getType() const { return self->getType(); }

    [[nodiscard]] std::string_view getName() const { return self->getName(); }

    [[nodiscard]] std::set<std::type_index> dependsOn() const { return self->dependsOn(); }

    [[nodiscard]] U apply(U unit) const { return self->apply(std::move(unit)); }

    bool operator==(const Rule<U>& other) const { return self->equals(*other.self); }

private:
    template <typename, typename>
    friend struct TypedRule;

    std::shared_ptr<const detail::ErasedRule<U>> self;
};

namespace detail
{
template <typename U, RuleConcept<U> RuleType>
struct RuleModel : ErasedRule<U>
{
    RuleType impl;

    explicit RuleModel(RuleType impl) : impl(std::move(impl)) { }

    [[nodiscard]] const std::type_info& getType() const override { return impl.getType(); }

    [[nodiscard]] std::string_view getName() const override { return impl.getName(); }

    [[nodiscard]] std::set<std::type_index> dependsOn() const override {return impl.dependsOn(); };

    [[nodiscard]] U apply(U unit) const override { return impl.apply(std::move(unit)); };

    [[nodiscard]] bool equals(const ErasedRule<U>& other) const override
    {
        if (auto ptr = dynamic_cast<const RuleModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

private:
    template <typename, typename>
    friend struct TypedRule;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, RuleType>)
        {
            return static_cast<const DynamicBase*>(&impl);
        }
        return std::nullopt;
    }
};
}

using PlanRule = Rule<LogicalPlan>;
template <typename T>
concept PlanRuleConcept = RuleConcept<T, LogicalPlan>;

}

template <typename U>
struct std::hash<NES::Rule<U>>
{
    size_t operator()(const NES::Rule<U>& rule) const noexcept { return std::hash<std::string_view>{}(rule.getName()); }
};
