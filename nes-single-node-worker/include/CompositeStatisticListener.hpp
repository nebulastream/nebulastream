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

#include <memory>
#include <vector>
#include <Listeners/StatisticListener.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{
/// Allows to register multiple statistics listener that are notified once the CompositeStatisticsListener gets notified
struct CompositeStatisticListener final : StatisticListener
{
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    void addQueryEngineListener(std::shared_ptr<QueryEngineStatisticListener> listener);
    void addSystemListener(std::shared_ptr<SystemEventListener> listener);
    void addListener(std::shared_ptr<StatisticListener> listener);
    [[nodiscard]] bool hasListeners() const;

private:
    std::vector<std::shared_ptr<QueryEngineStatisticListener>> queryEngineListeners;
    std::vector<std::shared_ptr<SystemEventListener>> systemListeners;
};
}
