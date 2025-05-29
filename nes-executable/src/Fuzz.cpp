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
#include <cpptrace/from_current.hpp>
#include "ErrorHandling.hpp"
#include "SQLQueryParser/AntlrSQLQueryParser.hpp"

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
        for (int num_excl_marks = 0; num_excl_marks < 20; ++num_excl_marks)
        {
            const auto* pre = "SELECT a FROM b WHERE ";
            std::string marks;
            for (int i = 0; i <= num_excl_marks; ++i)
            {
                marks += '!';
            }
            const auto* post = "c INTO d";

            auto query = pre + marks + post;

            auto begin = std::chrono::steady_clock::now();
            auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            auto end = std::chrono::steady_clock::now();

            fmt::println("{:>2} took {:>10}", num_excl_marks, std::chrono::duration_cast<std::chrono::milliseconds>(end - begin));
        }
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return 0;
    }
}
