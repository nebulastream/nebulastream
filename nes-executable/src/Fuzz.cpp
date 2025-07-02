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
#include <cstddef>
#include <cstdint>
#include "SQLQueryParser/AntlrSQLQueryParser.hpp"
#include "Util/PlanRenderer.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
{
    NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
    try
    {
        std::string query(reinterpret_cast<const char*>(data), size);
        NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
        return 0;
    }
    catch (...)
    {
        return 0;
    }
}
