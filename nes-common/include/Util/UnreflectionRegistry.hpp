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

#include <functional>
#include <optional>
#include <Util/ReflectionFwd.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// A RuntimeRegistry that dispatches deserialization by name without compile-time knowledge
/// of the concrete plugin types: each entry unreflects serialized data into ReturnTypeT.
/// Entries are populated by per-plugin glue translation units at static initialization time
/// (see cmake/UnreflectionRegistrationUtil.cmake, a thin wrapper around the generic
/// cmake/RuntimeRegistrationUtil.cmake).
template <typename ConcreteRegistry, typename KeyTypeT, typename ReturnTypeT>
class UnreflectionRegistry
    : public RuntimeRegistry<ConcreteRegistry, KeyTypeT, std::function<ReturnTypeT(const Reflected&, const ReflectionContext&)>>
{
protected:
    UnreflectionRegistry() = default;

public:
    using ReturnType = ReturnTypeT;
    using UnreflectorFn = std::function<ReturnTypeT(const Reflected&, const ReflectionContext&)>;

    [[nodiscard]] std::optional<ReturnTypeT> unreflect(const KeyTypeT& name, const Reflected& data, const ReflectionContext& context) const
    {
        if (const auto unreflector = this->find(name))
        {
            return (*unreflector)(data, context);
        }
        return std::nullopt;
    }
};

}
