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

#include <cpptrace/from_current.hpp>

#include <Runtime/QueryTerminationType.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
{
    NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);

    CPPTRACE_TRY
    {
        std::string query(reinterpret_cast<const char*>(data), size);
        auto dqp = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
        NES::SingleNodeWorker snw{NES::SingleNodeWorkerConfiguration{}};
        auto qid = snw.registerQuery(dqp);
        if (!qid)
        {
            throw qid.error();
        }
        snw.startQuery(*qid);
        snw.stopQuery(*qid, NES::QueryTerminationType::Graceful);
        while (true)
        {
            if (snw.getQuerySummary(*qid)->currentStatus <= NES::QueryStatus::Running)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            else
            {
                return 0;
            }
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return 0;
    }
    return 0;
}
