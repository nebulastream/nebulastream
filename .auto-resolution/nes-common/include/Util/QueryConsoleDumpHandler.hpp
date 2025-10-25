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
#include <memory>
#include <string>
#include <type_traits>
#include <Util/PlanRenderer.hpp>

template <typename T>
struct IsSharedPtr : std::false_type
{
};

template <typename T>
struct IsSharedPtr<std::shared_ptr<T>> : std::true_type
{
};

template <typename T>
constexpr bool is_shared_ptr_v = IsSharedPtr<T>::value;

namespace NES
{

template <typename Plan, typename Operator>
class QueryConsoleDumpHandler
{
public:
    explicit QueryConsoleDumpHandler(std::ostream& out, bool multiline) : out(out), multiline(multiline) { }

    void dump(const Operator& node) { dumpRecursive(node, 0, out, multiline); }

    static void dumpRecursive(const Operator& op, const uint64_t level, std::ostream& out, const bool multiline)
    {
        const std::string indent(level * 2, ' ');
        out << indent << (multiline ? "+ " : "") << op.explain(ExplainVerbosity::Debug) << '\n';
        for (auto& child : op.getChildren())
        {
            if constexpr (is_shared_ptr_v<std::decay_t<decltype(child)>>)
            {
                dumpRecursive(*child, level + 1, out, multiline);
            }
            else
            {
                dumpRecursive(child, level + 1, out, multiline);
            }
        }
    }

    void dumpPlan(const Plan& plan) { out << "Dumping query plan: " << plan.toString() << std::endl; }

private:
    std::ostream& out;
    bool multiline;
};

}
