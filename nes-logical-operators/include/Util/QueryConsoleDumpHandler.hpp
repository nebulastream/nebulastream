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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// @brief Converts query plans and pipeline plans to the .nesviz format and dumps them to a file.m

class QueryConsoleDumpHandler
{
public:
    virtual ~QueryConsoleDumpHandler() = default;
    explicit QueryConsoleDumpHandler(std::ostream& out);

    void dump(const LogicalOperator& node);
    void multilineDump(const LogicalOperator& node);

    /// @brief Dump a query plan with a specific context and scope.
    /// @param context the context
    /// @param scope the scope
    /// @param plan the query plan
    void dump(std::string context, std::string scope, LogicalPlan plan);

private:
    std::ostream& out;
    void dumpHelper(const LogicalOperator& operationNode, uint64_t depth, uint64_t indent, std::ostream& out) const;
    void multilineDumpHelper(const LogicalOperator& operationNode, uint64_t depth, uint64_t indent, std::ostream& out) const;
};

}
