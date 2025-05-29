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

#include <chrono>
#include <iostream>
#include <thread>
#include <libprotobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include "Runtime/QueryTerminationType.hpp"
#include "SerializableQueryPlan.pb.h"
#include "Serialization/QueryPlanSerializationUtil.hpp"
#include "SingleNodeWorker.hpp"
#include "SingleNodeWorkerConfiguration.hpp"

DEFINE_PROTO_FUZZER(const NES::SerializableQueryPlan& sqp)
{
    NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);

    if (sqp.rootoperatorids().empty())
    {
        return;
    }

    for (auto o : sqp.operatormap())
    {
        for (auto c : o.second.children_ids())
        {
            if (!sqp.operatormap().contains(c))
            {
                return;
            }
        }
    }

    std::cout << "---\n" << sqp.DebugString() << "---" << std::endl;
    auto dqp = NES::QueryPlanSerializationUtil::deserializeQueryPlan(sqp);
    NES::SingleNodeWorker snw{NES::Configuration::SingleNodeWorkerConfiguration{}};
    auto qid = snw.registerQuery(dqp);
    snw.startQuery(qid);
    snw.stopQuery(qid, NES::QueryTerminationType::Graceful);
    while (true)
    {
        if (snw.getQuerySummary(qid)->currentStatus <= NES::QueryStatus::Running)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        else
        {
            return;
        }
    }
}
