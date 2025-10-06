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
#include <type_traits>
#include "StateDefinitions.hpp"

namespace NES::State {

template<typename T>
concept SerializableState = requires {
    std::is_trivially_copyable_v<T> || std::is_base_of_v<BaseState, T>;
};

template<typename T>
concept AggregationLikeState = requires {
    std::is_same_v<T, AggregationState>;
};

template<typename T>
struct StateType {
    static constexpr bool isAggregation = std::is_same_v<T, AggregationState>;
    static constexpr bool isJoin = std::is_same_v<T, JoinState>;
    static constexpr bool isFilter = std::is_same_v<T, FilterState>;
    static constexpr bool isValid = isAggregation || isJoin || isFilter;
};

}