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
#include <iostream>
#include <cpptrace/from_current.hpp>
#include "ErrorHandling.hpp"
#include "SQLQueryParser/AntlrSQLQueryParser.hpp"
#include "Util/PlanRenderer.hpp"

int main()
{
    /// if logging enabled, setup logging
    if (NES_COMPILE_TIME_LOG_LEVEL > 1)
    {
        std::cout << "logging enabled" << std::endl;
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_INFO);
    }
    CPPTRACE_TRY
    {
        std::string query{
            "SELECT * FROM (SELECT * FROM stream0) JOIN (SELECT * FROM stream2) ON u_s1 = _s1 = i8_s2 "
            "and !!!!!!!!!!!!20Ants2 "
            "and !!!!!!!!!!odeFunctionE_s1 = i8_s2 and !!!!f64Wsi8_s2 and !!!s2 and !!!!f64Wsi8_s2 and !!!!!!!!!!!!8_s1BOO_s2 "
            "and u8N3NES19LogicalNodeFunctionE_s1 = i8_i8_s2 and !!!!f64Wsi8_s2 and !!!!!!!!!! Lists1 = i8_s2 and !!!!f64Wsi8_s2 "
            "and !!!!!!!!!!!!8_s1BOO_s2 and u8N3NES19LogicalNodeFunctionE_s1 = i8_s2 and !!!!f64Wsi8_s2 "
            "and !!!!!!!!!!!!8_sFunctionE_s1 = i8_s2 and !!!!f64Wsi8_s2 and !!!!!!!!!!!!8_s1BOO_s2 "
            "and u8N3NES19LogicalNodeFunctionE_s1 = i8_i8_s2 and !!!!f64Wsi8_s2 and !!!!!!!!!!!!8_s1BOO_s2 "
            "and u8N3NES19LogicalNodeFunctionE_s1 = i8_i8_s2 and !!!!f64Wsi8_s2 and !!!!!!!!!!!!8_s1BOO_s2 "
            "and u8N3NES19LogicalNodeFunctionE_odeFunctionE_s1 = i8_s2 and !!!!f64Ws2 "
            "WINDOW TUMBLING (ts, size 1 sec) INTO sinkStream2Stream2"};
        NES_INFO("foo");
        std::cout << "starting " << query.size() << std::endl;
        auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
        std::cout << "done" << std::endl;
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return 0;
    }
}
