//
// Created by ls on 4/18/25.
//

#pragma once

#include <algorithm>
#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <COW.hpp>

namespace NES
{

class DataTypeImpl
{
public:
    virtual ~DataTypeImpl() = default;
    using NameT = std::string_view;

    [[nodiscard]] virtual std::shared_ptr<DataTypeImpl> clone() const = 0;
    [[nodiscard]] virtual NameT getName() const = 0;
    bool operator==(const DataTypeImpl&) const = default;
    virtual bool softEqual(const DataTypeImpl&) const = 0;

protected:
    virtual bool equals(const DataTypeImpl&) const = 0;
};

template <typename T>
class DataTypeImplHelper : public DataTypeImpl
{
public:
    using NameT = std::string_view;

    [[nodiscard]] std::shared_ptr<DataTypeImpl> clone() const override
    {
        if constexpr (std::is_copy_constructible_v<T>)
        {
            return std::make_shared<T>(static_cast<const T&>(*this));
        }
        else if constexpr (std::is_default_constructible_v<T>)
        {
            return std::make_shared<T>();
        }
        else
        {
            static_assert(false, "clone can only be implemented if class is copy or default constructible");
            return nullptr;
        }
    }

    NameT getName() const override { return T::Name; }

    bool softEqual(const DataTypeImpl& other) const override { return typeid(T) == typeid(other); }

protected:
    bool equals(const DataTypeImpl& other) const override
    {
        if (typeid(T) == typeid(other))
        {
            return static_cast<const T&>(*this) == static_cast<const T&>(other);
        }
        return false;
    }
};


namespace detail
{
struct Scalar
{
    bool operator==(const Scalar&) const = default;
};
struct VarSized
{
    bool operator==(const VarSized&) const = default;
};
struct FixedSize
{
    bool operator==(const FixedSize&) const = default;
    size_t size;
};
using Shape = std::variant<Scalar, VarSized, FixedSize>;
}

class DataType
{
    COW<DataTypeImpl> impl;
    struct TypeProperties
    {
        bool nullable = false;
        detail::Shape shape;
        bool operator==(const TypeProperties&) const = default;
    };
    TypeProperties properties;

public:
    using Name = DataTypeImpl::NameT;
    Name name() const { return impl->getName(); }
    explicit DataType(COW<DataTypeImpl> impl, bool nullable, detail::Shape shape)
        : impl(std::move(impl)), properties(nullable, std::move(shape))
    {
    }
    bool isNullable() const { return properties.nullable; }
    bool isScalar() const { return std::holds_alternative<detail::Scalar>(properties.shape); }

    bool operator==(const DataType& other) const { return properties == other.properties && *impl == *other.impl; }
    std::optional<size_t> isFixedSize() const
    {
        if (auto* fixedSize = std::get_if<detail::FixedSize>(&properties.shape))
        {
            return fixedSize->size;
        }
        return std::nullopt;
    }

    bool isVarsized() const { return std::holds_alternative<detail::VarSized>(properties.shape); }
    bool isList() const
    {
        return std::holds_alternative<detail::FixedSize>(properties.shape) || std::holds_alternative<detail::FixedSize>(properties.shape);
    }

    [[nodiscard]] DataType asList(size_t size) const
    {
        auto type = *this;
        type.properties.shape = detail::FixedSize{.size = size};
        return type;
    }

    [[nodiscard]] DataType asList() const
    {
        auto type = *this;
        type.properties.shape = detail::VarSized{};
        return type;
    }

    [[nodiscard]] DataType asNullable() const
    {
        auto type = *this;
        type.properties.nullable = true;
        return type;
    }

    template <std::derived_from<DataTypeImpl> T>
    const T& typeProperties() const
    {
        return dynamic_cast<const T&>(*impl);
    }
    friend struct DataTypeInspector;
    friend class DataTypeSerializationFactory;
};


struct DataTypeInspector
{
    const DataType& type;
    explicit DataTypeInspector(const DataType& type) : type(type) { }
    DataTypeImpl::NameT name() const { return type.name(); }
    const detail::Shape& shape() const { return type.properties.shape; }
    bool nullable() const { return type.properties.nullable; }
};

inline auto format_as(const DataType& type)
{
    std::stringstream ss;
    if (type.isList())
    {
        ss << "List<";
    }
    if (type.isNullable())
    {
        ss << "Nullable<";
    }
    ss << type.name();
    if (type.isNullable())
    {
        ss << ">";
    }
    if (type.isList())
    {
        if (auto size = type.isFixedSize())
        {
            ss << ", " << *size;
        }
        ss << ">";
    }

    return ss.str();
}

DataType constant(std::string value);

class DataTypeRegistry;
void registerAllTypes(DataTypeRegistry& registry);

class DataTypeRegistry
{
    static DataTypeRegistry init();
    using DataTypeCreator = std::function<DataType()>;
    std::unordered_map<DataType::Name, DataTypeCreator> registryImpl;

public:
    static const DataTypeRegistry& instance()
    {
        static const DataTypeRegistry instance = init();
        return instance;
    }

    std::optional<DataType> lookup(DataType::Name name) const
    {
        if (auto it = registryImpl.find(name); it != registryImpl.end())
        {
            return it->second();
        }
        return {};
    }

    /// Mutable Access to Registry
    void registerDataType(DataType::Name name, DataTypeCreator creator)
    {
        if (!registryImpl.try_emplace(name, creator).second)
        {
            throw std::runtime_error("Type was registered multiple times");
        }
    }

};

class Function
{
public:
    using NameT = std::string_view;
    NameT name;
    DataType returnType;
};

class FunctionRegistry;
void registerAllFunctions(FunctionRegistry& registry, const DataTypeRegistry& types);

using MatcherFn = std::function<bool(const DataTypeInspector&)>;
using MatcherCreatorFn = std::function<MatcherFn()>;

class FunctionArgumentMatcher
{
    MatcherCreatorFn function;
    mutable std::optional<MatcherFn> current;

public:
    explicit FunctionArgumentMatcher(MatcherCreatorFn creatorFn) : function(std::move(creatorFn)) { }
    bool matches(const DataType& data) const { return (*current)(DataTypeInspector(data)); }
    void reset() const { current = function(); }
};

class FunctionSignatureMatcher
{
    Function::NameT name;
    std::vector<FunctionArgumentMatcher> argsMatcher;
    std::optional<FunctionArgumentMatcher> varArgsMatcher;

public:
    bool matches(Function::NameT name, std::span<const DataType> parameters) const
    {
        for (const auto& args_matcher : argsMatcher)
        {
            args_matcher.reset();
        }
        if (varArgsMatcher)
        {
            varArgsMatcher->reset();
        }

        if (name != this->name)
        {
            return false;
        }

        if (parameters.size() < argsMatcher.size())
        {
            return false;
        }

        if (parameters.size() > argsMatcher.size() && !varArgsMatcher)
        {
            return false;
        }

        auto nonVarArgs = parameters.subspan(0, argsMatcher.size());
        auto nonVarArgsMatch = std::ranges::all_of(
            std::views::zip(nonVarArgs, argsMatcher),
            [](const auto& argAndMatcher)
            {
                auto& [arg, matcher] = argAndMatcher;
                return matcher.matches(arg);
            });

        if (!nonVarArgsMatch)
        {
            return false;
        }

        auto varArgs = parameters.subspan(argsMatcher.size());
        return std::ranges::all_of(varArgs, [&](const auto& varArgs) { return varArgsMatcher.value().matches(varArgs); });
    }

    FunctionSignatureMatcher(
        Function::NameT name,
        std::vector<FunctionArgumentMatcher> args_matcher,
        std::optional<FunctionArgumentMatcher> var_args_matcher = {})
        : name(name), argsMatcher(std::move(args_matcher)), varArgsMatcher(std::move(var_args_matcher))
    {
    }
};

namespace Matcher
{
inline FunctionArgumentMatcher Scalar(DataTypeImpl::NameT name)
{
    return FunctionArgumentMatcher([name]() -> MatcherFn { return [name](const DataTypeInspector& type) { return type.name() == name; }; });
}

inline FunctionArgumentMatcher ListOf(DataTypeImpl::NameT name)
{
    return FunctionArgumentMatcher(
        [name]() { return [name](const DataTypeInspector& inspector) { return inspector.name() == name && inspector.type.isList(); }; });
}

inline FunctionArgumentMatcher FixedListOf(DataTypeImpl::NameT name)
{
    return FunctionArgumentMatcher(
        [name]() { return [name](const DataTypeInspector& type) { return type.name() == name && type.type.isFixedSize(); }; });
}

inline FunctionArgumentMatcher VarsizedListOf(DataTypeImpl::NameT name)
{
    return FunctionArgumentMatcher(
        [name]() { return [name](const DataTypeInspector& type) { return type.name() == name && type.type.isVarsized(); }; });
}

inline FunctionArgumentMatcher AnyFixedList()
{
    return FunctionArgumentMatcher([]() { return [](const DataTypeInspector& type) { return type.type.isFixedSize().has_value(); }; });
}

inline FunctionArgumentMatcher AnyVarsizedList()
{
    return FunctionArgumentMatcher([]() { return [](const DataTypeInspector& type) { return type.type.isVarsized(); }; });
}

inline FunctionArgumentMatcher AnyList()
{
    return FunctionArgumentMatcher(
        []() { return [](const DataTypeInspector& type) { return type.type.isFixedSize() || type.type.isVarsized(); }; });
}

inline FunctionArgumentMatcher AnyNonNullable()
{
    return FunctionArgumentMatcher([]() { return [](const DataTypeInspector& type) { return !type.type.isNullable(); }; });
}

inline FunctionArgumentMatcher AnyNullable()
{
    return FunctionArgumentMatcher([]() { return [](const DataTypeInspector& type) { return type.type.isNullable(); }; });
}

inline FunctionArgumentMatcher AnyEqual()
{
    return FunctionArgumentMatcher(
        []()
        {
            return [name = std::optional<DataTypeImpl::NameT>{}, shape = std::optional<detail::Shape>{}, nullable = std::optional<bool>{}](
                       const DataTypeInspector& type) mutable
            {
                if (!name)
                {
                    name = type.name();
                    shape = type.shape();
                    nullable = type.nullable();
                }
                return *name == type.name() && *shape == type.shape() && *nullable == type.nullable();
            };
        });
}
}

namespace Util
{
inline bool any_nullable(std::span<const DataType> types)
{
    return std::ranges::any_of(types, &DataType::isNullable);
}
}

class FunctionRegistry
{
    static FunctionRegistry init()
    {
        FunctionRegistry registry;
        registerAllFunctions(registry, DataTypeRegistry::instance());
        return registry;
    }

    using FunctionCreator = std::function<Function(std::span<const DataType>)>;
    std::vector<std::pair<FunctionSignatureMatcher, FunctionCreator>> registryImpl;

public:
    static const FunctionRegistry& instance()
    {
        static FunctionRegistry registry = init();
        return registry;
    }

    std::optional<Function> lookup(Function::NameT name, std::span<const DataType> arguments) const
    {
        auto it = std::ranges::find_if(registryImpl, [&](const auto& pair) { return pair.first.matches(name, arguments); });

        if (it != registryImpl.end())
        {
            return it->second(arguments);
        }
        return {};
    }

    void registerFunction(Function::NameT name, std::vector<FunctionArgumentMatcher> arguments, FunctionCreator creator)
    {
        registryImpl.emplace_back(FunctionSignatureMatcher(name, std::move(arguments)), creator);
    }

    void registerFunction(
        Function::NameT name,
        std::vector<FunctionArgumentMatcher> arguments,
        FunctionArgumentMatcher varArgMatcher,
        FunctionCreator creator)
    {
        registryImpl.emplace_back(FunctionSignatureMatcher(name, std::move(arguments), std::move(varArgMatcher)), creator);
    }
};


template <typename T, std::convertible_to<T>... Ts>
std::vector<T> make_vector(Ts&&... ts)
{
    std::vector<T> vector;
    vector.reserve(sizeof...(Ts));
    (vector.emplace_back(static_cast<T>(std::forward<Ts>(ts))), ...);
    return vector;
}
}