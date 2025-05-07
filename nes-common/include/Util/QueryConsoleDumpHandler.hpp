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
#include <functional>
#include <iostream>
#include <vector>
#include <Util/PlanRenderer.hpp>

namespace
{
template <typename T>
struct is_shared_ptr : std::false_type
{
};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type
{
};

template <typename T>
constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;
}

namespace NES
{

template <typename Plan, typename Operator>
class QueryConsoleDumpHandler
{
public:
    explicit QueryConsoleDumpHandler(std::ostream& out, bool multiline = false) : out(out), multiline(multiline) { }

    void dump(const std::shared_ptr<Operator>& node) { dump(*node); }

    void dump(const Operator& node)
    {
        std::function<void(const Operator&, uint64_t)> dumpRecursive = [this, &dumpRecursive](const Operator& op, uint64_t level)
        {
            std::string indent(level * 2, ' ');

            if (multiline)
            {
                out << indent << "+ " << op.explain(ExplainVerbosity::Debug) << "\n";
            }
            else
            {
                out << indent << op.explain(ExplainVerbosity::Debug) << "\n";
            }

            for (auto& child : op.getChildren())
            {
                if constexpr (is_shared_ptr_v<std::decay_t<decltype(child)>>)
                {
                    dumpRecursive(*child, level + 1);
                }
                else
                {
                    dumpRecursive(child, level + 1);
                }
            }
        };

        dumpRecursive(node, 0);
    }

    void dumpPlan(const Plan& plan) { out << "Dumping query plan: " << plan.toString() << std::endl; }

private:
    std::ostream& out;
    bool multiline;
};

}
