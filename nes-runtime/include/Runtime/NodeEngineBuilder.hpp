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
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/NodeEngine.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{
/// Create instances of NodeEngine using the builder pattern.
class NodeEngineBuilder
{
public:
    NodeEngineBuilder() = delete;

    explicit NodeEngineBuilder(
        const WorkerConfiguration& workerConfiguration,
        std::shared_ptr<SystemEventListener> systemEventListener,
        std::vector<std::shared_ptr<QueryEngineStatisticListener>> statisticListener);

    std::unique_ptr<NodeEngine> build();

private:
    WorkerConfiguration workerConfiguration;
    std::shared_ptr<SystemEventListener> systemEventListener;
    std::vector<std::shared_ptr<QueryEngineStatisticListener>> statisticEventListener;
};
}
