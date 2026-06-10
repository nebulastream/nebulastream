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


#include <Debug/DebugHelpers.hpp>

#include <sstream>
#include <string>

#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>

namespace NES::Debug
{

std::string dump(const LogicalPlan& plan)
{
    return explain(plan, ExplainVerbosity::Debug);
}

std::string dump(const LogicalOperator& op)
{
    std::ostringstream oss;
    QueryConsoleDumpHandler<LogicalPlan, LogicalOperator>::dumpRecursive(op, 0, oss, true);
    return oss.str();
}

// Force the linker to keep these symbols even though they are not called
// by any production code. They are intended for use in the debugger.
    __attribute__((used)) std::string (*_dump_plan)(const LogicalPlan&) = &dump;
    __attribute__((used)) std::string (*_dump_op)(const LogicalOperator&) = &dump;
} // namespace NES::Debug
