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

///NOLINTBEGIN(misc-include-cleaner)
#include <cstddef>
#include <utility>
#include <Util/Reflection/NESStrongTypeReflection.hpp>
#include <Util/Reflection/OptionalReflection.hpp>
#include <Util/Reflection/PairReflection.hpp>
#include <Util/Reflection/ReflectionCore.hpp>
#include <Util/Reflection/StringViewReflection.hpp>
#include <Util/Reflection/UnorderedMapReflection.hpp>
#include <Util/Reflection/VariantReflection.hpp>
#include <Util/Reflection/VectorReflection.hpp>
#include <Util/ReflectionFwd.hpp>
#include <rfl/Generic.hpp>
#include <rfl/Tuple.hpp>
#include <rfl/named_tuple_t.hpp>

///NOLINTEND(misc-include-cleaner)

namespace NES
{

/// This header provides the complete reflection system including:
/// - Core reflection functionality (ReflectionCore.hpp)
/// - Reflector/Unreflector specializations for standard library types:
///   - std::vector
///   - std::optional
///   - std::pair
///   - std::variant
///   - std::unordered_map
///
/// For forward declarations only, use ReflectionFwd.hpp
/// For core functionality without container specializations, use ReflectionCore.hpp

}
