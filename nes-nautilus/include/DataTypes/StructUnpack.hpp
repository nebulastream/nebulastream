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

#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include <DataTypes/DataTypesUtil.hpp>
#include <rfl/to_view.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Reads every member of the aggregate Struct behind structPointer back into nautilus values, in declaration
/// order, deriving the member types and offsets via reflect-cpp. Proxy functions can only return a single
/// value, so multi-value results travel as a pointer to a (thread-local) result struct; this helper replaces
/// the per-member getMemberWithOffset/offsetof readback after such an invoke:
///     const auto [value, isNull] = unpackStruct(parseResult);
template <typename Struct>
requires(std::is_aggregate_v<Struct> and std::is_default_constructible_v<Struct>)
auto unpackStruct(const nautilus::val<Struct*>& structPointer)
{
    /// reflect-cpp reflects instances, not types, so member offsets are taken from a static dummy instance.
    static const Struct instance{};
    const auto view = rfl::to_view(instance);
    return [&]<std::size_t... MemberIndices>(std::index_sequence<MemberIndices...>)
    {
        /// Braced tuple construction guarantees left-to-right evaluation, keeping the traced loads in
        /// declaration order.
        return std::tuple{[&]
                          {
                              const auto* memberPointer = view.template get<MemberIndices>();
                              using Member = std::remove_const_t<std::remove_pointer_t<std::remove_cvref_t<decltype(memberPointer)>>>;
                              const auto offset = reinterpret_cast<const char*>(memberPointer) - reinterpret_cast<const char*>(&instance);
                              return nautilus::val<Member>{*getMemberWithOffset<Member>(structPointer, offset)};
                          }()...};
    }(std::make_index_sequence<std::remove_cvref_t<decltype(view)>::size()>{});
}

}
