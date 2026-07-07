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

#include <type_traits>
#include <Interface/VariableSizedAccess.hpp>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>

namespace nautilus
{

/// We are specializing the nautilus::val<> implementation so that we can use nautilus::val<VariableSizedAccess>
template <>
class val<NES::VariableSizedAccess>
{
public:
    /// Friend declarations for the specializations
    template <typename LHS>
    friend struct details::RawValueResolver;

    template <typename T>
    friend struct details::StateResolver;

    using Underlying = NES::VariableSizedAccess;

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    explicit val(const Underlying variableSizedAccess)
        : index(variableSizedAccess.getIndex().getRawValue())
        , offset(variableSizedAccess.getOffset().getRawOffset())
        , size(variableSizedAccess.getSize().getRawSize())
    {
    }

    val(const val& other) = default;
    val& operator=(const val& other) = default;

private:
    val<uint32_t> index;
    val<uint32_t> offset;
    val<uint64_t> size;
};

}
