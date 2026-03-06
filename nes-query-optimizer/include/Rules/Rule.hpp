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
#include <string>
#include <typeinfo>

#include "Plans/LogicalPlan.hpp"

#include "Rule.hpp"

namespace NES
{
namespace detail
{
struct ErasedRule;
}

template <typename Checked = detail::ErasedRule>
struct TypedRule;
using Rule = TypedRule<>;

template <typename T>
concept RuleConcept = requires(const T& thisRule, LogicalPlan plan, const T& rhs)
{

    { thisRule.getType() } -> std::convertible_to<const std::type_info&>;
    { thisRule.getName() } -> std::convertible_to<std::string_view>;

    { thisRule.apply(plan) } -> std::same_as<LogicalPlan>;

    { thisRule.hash() } -> std::convertible_to<std::size_t>;
    { thisRule == rhs } -> std::convertible_to<bool>;
};

namespace detail
{
struct ErasedRule
{
    virtual ~ErasedRule() = default;

    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    [[nodiscard]] virtual std::string_view getName() const = 0;

    [[nodiscard]] virtual LogicalPlan apply(LogicalPlan plan) const = 0;

    [[nodiscard]] virtual std::size_t hash() const = 0;
    [[nodiscard]] virtual bool equals(const ErasedRule& other) const = 0;
    friend bool operator==(const ErasedRule& lhs, const ErasedRule& rhs) { return lhs.equals(rhs); }

private:
    template <typename T>
    friend struct NES::TypedRule;
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <RuleConcept RuleType>
struct RuleModel;
}

template<typename Checked>
struct TypedRule
{
    template <RuleConcept T>
    requires std::same_as<Checked, detail::ErasedRule>
    TypedRule(TypedRule<T> other): self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    template <typename T>
    TypedRule(const T& rule) : self(std::make_shared<detail::RuleModel<T>>(rule)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <RuleConcept T>
    TypedRule(const detail::RuleModel<T>& rule) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<detail::RuleModel<T>>(rule.impl))
    {
    }

    explicit TypedRule(std::shared_ptr<const detail::ErasedRule> rule) : self(std::move(rule))
    {
    }

    TypedRule() = delete;

    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<Checked, detail::ErasedRule>)
        {
            return *std::dynamic_pointer_cast<const detail::ErasedRule>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<detail::RuleModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        return get();
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<detail::ErasedRule, Checked>)
        {
            return std::dynamic_pointer_cast<const detail::ErasedRule>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const detail::RuleModel<Checked>>(self);
            return &casted->impl;
        }
    }

    template <RuleConcept T>
    std::optional<TypedRule<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const detail::RuleModel<T>>(self))
        {
            return TypedRule<T>{std::static_pointer_cast<const detail::ErasedRule>(model)};
        }
        return std::nullopt;
    }

    template <typename T>
    requires(!RuleConcept<T>)
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

    template <RuleConcept T>
    TypedRule<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const detail::RuleModel<T>>(self))
        {
            return TypedRule<T>{std::static_pointer_cast<const detail::ErasedRule>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    template <typename T>
    requires(!RuleConcept<T>)
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

    [[nodiscard]] const std::type_info& getType() const {return self->getType(); }
    [[nodiscard]] std::string_view getName() const { return self->getName(); }
    [[nodiscard]] LogicalPlan apply(LogicalPlan plan) const { return self->apply(std::move(plan)); }
    [[nodiscard]] size_t hash() const { return self->hash(); }
    bool operator==(const Rule& other) const { return self->equals(*other.self); }

private:
    template <typename FriendChecked>
    friend struct TypedRule;

    std::shared_ptr<const detail::ErasedRule> self;
};

namespace detail
{
template <RuleConcept RuleType>
struct RuleModel : ErasedRule
{
    RuleType impl;

    explicit RuleModel(RuleType impl) : impl(std::move(impl)) { }


    [[nodiscard]] const std::type_info& getType() const override { return impl.getType(); }
    [[nodiscard]] std::string_view getName() const override { return impl.getName(); }

    [[nodiscard]] LogicalPlan apply(LogicalPlan plan) const override { return impl.apply(std::move(plan)); };

    [[nodiscard]] size_t hash() const override { return impl.hash(); }
    [[nodiscard]] bool equals(const ErasedRule& other) const override
    {
        if (auto ptr = dynamic_cast<const RuleModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

private:
    template<typename T>
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
}

template<>
struct std::hash<NES::Rule>
{
    size_t operator()(const NES::Rule& rule) const noexcept { return rule.hash(); }
};