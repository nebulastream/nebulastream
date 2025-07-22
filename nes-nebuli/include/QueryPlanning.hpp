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

#include <algorithm>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Distributed/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <YAML/YamlBinder.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class QueryPlanner
{
public:
    struct FinalizedLogicalPlan
    {
        explicit FinalizedLogicalPlan(QueryDecomposer::DecomposedLogicalPlan&& plan) : plan(std::move(plan))
        {
            PRECONDITION(not this->plan.empty(), "Input plan should not be empty");
        }

        const LogicalPlan& operator*() const { return plan.begin()->second.front(); }

        const LogicalPlan& operator*() { return plan.begin()->second.front(); }

        const LogicalPlan* operator->() const { return &(**this); }

        // Subscript operator for accessing plans by its grpc addr
        const std::vector<LogicalPlan>& operator[](const CLI::GrpcAddr& connection) const
        {
            const auto it = plan.find(connection);
            if (it == plan.end())
            {
                throw std::out_of_range(fmt::format("No plan found in decomposed plan under addr {}", connection));
            }
            return it->second;
        }

        std::vector<LogicalPlan>& operator[](const CLI::GrpcAddr& connection) { return plan.at(connection); }

        size_t size() const { return plan.size(); }
        auto view() const { return plan; }
        auto begin() const { return plan.cbegin(); }
        auto end() const { return plan.cend(); }

    private:
        QueryDecomposer::DecomposedLogicalPlan plan;
    };

    static FinalizedLogicalPlan plan(BoundLogicalPlan&& boundPlan);
};

template <class T>
[[nodiscard]] std::vector<T> getOperatorByType(const QueryPlanner::FinalizedLogicalPlan& distributedPlan)
{
    return distributedPlan | std::views::values | std::views::join
        | std::views::transform([](const auto& localPlan) { return getOperatorByType<T>(localPlan); }) | std::views::join
        | std::ranges::to<std::vector>();
}

}