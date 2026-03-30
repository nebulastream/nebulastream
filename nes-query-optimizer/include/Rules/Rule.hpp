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


#include <memory>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>

namespace NES
{
class Rule
{
public:
    virtual ~Rule() = default;
    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    [[nodiscard]] virtual std::string_view getName() const = 0;

    [[nodiscard]] virtual std::set<std::type_index> dependsOn() const = 0;
    [[nodiscard]] virtual std::set<std::type_index> requiredBy() const = 0;

    [[nodiscard]] virtual bool equals(const Rule& other) const = 0;

    friend bool operator==(const Rule& lhs, const Rule& rhs) { return lhs.equals(rhs); }
};
}