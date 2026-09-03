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
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/StatisticListener.hpp>
#include <Runtime/BufferProviderStatisticListener.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES
{
/// Create instances of NodeEngine using the builder pattern.
class NodeEngineBuilder
{
public:
    NodeEngineBuilder() = delete;

    /// @param bufferEventListener optional observer of the buffer providers. It is separate from
    /// statisticListener because buffer events are not per query and fire on the buffer handoff path: passing
    /// null keeps that path free of a virtual call.
    explicit NodeEngineBuilder(
        WorkerConfiguration workerConfiguration,
        std::shared_ptr<StatisticListener> statisticListener,
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations): defaulted so that the many existing callers stay untouched.
        std::shared_ptr<BufferProviderStatisticListener> bufferEventListener = nullptr);

    std::unique_ptr<NodeEngine> build(const Host& host);

private:
    WorkerConfiguration workerConfiguration;
    std::shared_ptr<StatisticListener> statisticsListener;
    std::shared_ptr<BufferProviderStatisticListener> bufferEventListener;
};
}
