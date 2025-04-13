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
#include <iostream>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <QueryConsoleDumpHandler.hpp>
#include <fmt/format.h>

namespace NES
{

/// @brief Converts query plans and pipeline plans to the .nesviz format and dumps them to a file.
template<typename Plan, typename Operator>
class QueryConsoleDumpHandler
{
public:
    virtual ~QueryConsoleDumpHandler() = default;
    explicit QueryConsoleDumpHandler(std::ostream& out) : out(out) {}

    void dump(const Operator& node)
    {
        multilineDumpHelper(node, /*depth*/ 0, /*indent*/ 2, out);
    }

    void multilineDump(const Operator& node)
    {
        multilineDumpHelper(node, /*depth*/ 0, /*indent*/ 2, out);
    }

    void dump(Plan plan)
    {
        out << "Dumping queryPlan: " << plan.toString() << std::endl;
    }

private:
    std::ostream& out;
    void dumpHelper(const Operator& operationNode, uint64_t depth, uint64_t indent, std::ostream& out) const
    {
        out << std::string(indent * depth, ' ') << operationNode << '\n';
        ++depth;
        for (auto& child : operationNode.getChildren())
        {
            dumpHelper(child, depth, indent, out);
        }
    }

    void multilineDumpHelper(const Operator& operationNode, uint64_t depth, uint64_t indent, std::ostream& out) const
    {
        std::vector<std::string> multiLineNodeString = {fmt::format("{}", operationNode)};
        for (const std::string& line : multiLineNodeString)
        {
            for (auto i{0ULL}; i < indent * depth; ++i)
            {
                if (i % indent == 0)
                {
                    out << '|';
                }
                else
                {
                    if (line == multiLineNodeString.front() && i >= indent * depth - 1)
                    {
                        out << std::string(indent, '-');
                    }
                    else
                    {
                        out << std::string(indent, ' ');
                    }
                }
            }
            if (line != multiLineNodeString.front())
            {
                out << '|' << ' ';
            }
            out << line << std::endl;
        }
        ++depth;
        for (auto& child : operationNode.getChildren())
        {
            multilineDumpHelper(child, depth, indent, out);
        }
    }
};

}
