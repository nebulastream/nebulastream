/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_QUERYCOMPILER_SINKRECONFIGURATION_HPP_
#define INCLUDE_QUERYCOMPILER_SINKRECONFIGURATION_HPP_

#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/ReconfigurationTask.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/SinksForwaredRefs.hpp>

namespace NES::NodeEngine {

/**
 * @brief The query compiler compiles physical query plans to an executable query plan
 */
class SinkReconfiguration : public Reconfigurable {
  public:
    SinkReconfiguration(std::vector<NES::DataSinkPtr> sinks, NES::NodeEngine::Execution::ExecutableQueryPlanPtr qep);
    ~SinkReconfiguration() = default;

    void setup(QueryManagerPtr queryManager, QuerySubPlanId id);

    void destroyCallback(NES::NodeEngine::ReconfigurationTask& task) override;

    void reconfigure(NES::NodeEngine::ReconfigurationTask&, NES::NodeEngine::WorkerContext& context) override;

  private:
    std::vector<NES::DataSinkPtr> sinks;
    NES::NodeEngine::Execution::ExecutableQueryPlanPtr qep;
};

}// namespace NES::NodeEngine

#endif//INCLUDE_QUERYCOMPILER_SINKRECONFIGURATION_HPP_