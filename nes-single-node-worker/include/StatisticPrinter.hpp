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

#include <filesystem>
#include <fstream>
#include <thread>
#include <type_traits>
#include <variant>
#include <Listeners/SystemEventListener.hpp>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>

template <typename Var1, typename Var2>
struct FlattenVariant;

template <typename... Ts1, typename... Ts2>
struct FlattenVariant<std::variant<Ts1...>, std::variant<Ts2...>>
{
    using type = std::variant<Ts1..., Ts2...>;
};

namespace NES::Runtime
{
struct PrintingStatisticListener final : QueryEngineStatisticListener, SystemEventListener
{
    using CombinedEventType = FlattenVariant<SystemEvent, Event>::type;
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    explicit PrintingStatisticListener(const std::filesystem::path& path);
    static_assert(std::is_default_constructible_v<CombinedEventType>);

private:
    std::ofstream file;
    folly::MPMCQueue<CombinedEventType> events{100};
    std::jthread printThread;
};
}
