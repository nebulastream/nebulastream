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
#include <utility>
#include <Sources/AsyncOperator.hpp>
#include <Util/Reflection.hpp>
#include <AsyncOperatorUnreflectionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
struct ReflectedAsyncOperator
{
    std::string type;
    Reflected config;
};

inline AsyncOperator unreflectAsyncOperator(const Reflected& data, const ReflectionContext& context)
{
    const auto wire = context.unreflect<ReflectedAsyncOperator>(data);
    auto config = AsyncOperatorUnreflectionRegistry::instance().unreflect(wire.type, wire.config, context);
    if (!config)
    {
        throw CannotDeserialize("No async operator unreflection entry registered for {}", wire.type);
    }
    if (config->getType() != wire.type)
    {
        throw CannotDeserialize("Async operator config type {} does not match envelope {}", config->getType(), wire.type);
    }
    return std::move(*config);
}
}

template <typename Checked>
struct Reflector<TypedAsyncOperator<Checked>>
{
    Reflected operator()(const TypedAsyncOperator<Checked>& op, const ReflectionContext& context) const
    {
        return context.reflect(detail::ReflectedAsyncOperator{op.getType(), op.reflect(context)});
    }
};

template <typename Checked>
struct Unreflector<TypedAsyncOperator<Checked>>
{
    TypedAsyncOperator<Checked> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        auto op = detail::unreflectAsyncOperator(data, context);
        if constexpr (std::same_as<Checked, detail::ErasedAsyncOperator>)
        {
            return op;
        }
        else
        {
            auto typed = op.template tryGetAs<Checked>();
            if (!typed)
            {
                throw CannotDeserialize("Async operator type {} does not match requested concrete type", op.getType());
            }
            return *typed;
        }
    }
};
}
