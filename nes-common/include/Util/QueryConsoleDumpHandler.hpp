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

namespace NES
{

namespace {

template<typename, typename = std::void_t<>>
struct has_getChildren : std::false_type {};

template<typename T>
struct has_getChildren<T, std::void_t<decltype(std::declval<const T&>().getChildren())>> : std::true_type {};

template<typename, typename = std::void_t<>>
struct has_getChild : std::false_type {};

template<typename T>
struct has_getChild<T, std::void_t<decltype(std::declval<const T&>().getChild())>> : std::true_type {};

template<typename T>
auto getChildOrChildren(const T& op) {
    if constexpr (has_getChildren<T>::value) {
        return op.getChildren();
    } else if constexpr (has_getChild<T>::value) {
        using ChildT = std::decay_t<decltype(*op.getChild())>;
        std::vector<ChildT> result;
        if (auto maybeChild = op.getChild(); maybeChild.has_value()) {
            result.push_back(maybeChild.value());
        }
        return result;
    } else {
        static_assert(has_getChildren<T>::value || has_getChild<T>::value,
                      "Operator type must have getChild or getChildren");
    }
}

}

template<typename Plan, typename Operator>
class QueryConsoleDumpHandler
{
public:
    explicit QueryConsoleDumpHandler(std::ostream& out, bool multiline = false)
        : out(out), multiline(multiline) {}

    void dump(const Operator& node)
    {
        std::function<void(const Operator&, uint64_t)> dumpRecursive =
            [this, &dumpRecursive](const Operator& op, uint64_t level) {
                std::string indent(level * 2, ' ');

                if (multiline)
                {
                    out << indent << "+ " << op << "\n";
                }
                else
                {
                    out << indent << op << "\n";
                }

                for (auto& child : getChildOrChildren(op)) {
                    dumpRecursive(child, level + 1);
                }
            };

        dumpRecursive(node, 0);
    }

    void dumpPlan(const Plan& plan)
    {
        out << "Dumping query plan: " << plan.toString() << std::endl;
    }

private:
    std::ostream& out;
    bool multiline;
};

}
