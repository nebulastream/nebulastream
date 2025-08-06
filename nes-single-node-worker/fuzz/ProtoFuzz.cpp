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
#include <thread>

#include <libprotobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <cpptrace/from_current.hpp>
#include <ErrorHandling.hpp>
#include <SerializableQueryPlan.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

DEFINE_PROTO_FUZZER(const NES::SerializableQueryPlan& sqp)
{
    NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);

    CPPTRACE_TRY
    {
        auto dqp = NES::QueryPlanSerializationUtil::deserializeQueryPlan(sqp);
        NES::SingleNodeWorker snw{NES::SingleNodeWorkerConfiguration{}};
        auto qid = snw.registerQuery(dqp);
        if (!qid)
        {
            throw qid.error();
        }
        auto r1 = snw.startQuery(*qid);
        if (!r1)
        {
            throw r1.error();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto r2 = snw.stopQuery(*qid, NES::QueryTerminationType::Graceful);
        if (!r2)
        {
            throw r2.error();
        }
        while (true)
        {
            if (snw.getQuerySummary(*qid)->currentStatus <= NES::QueryStatus::Running)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            else
            {
                return;
            }
        }
    }
    CPPTRACE_CATCH(...)
    {
        return;
    }
}
