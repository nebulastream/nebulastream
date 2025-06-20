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
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include "Listeners/QueryLog.hpp"

namespace NES
{
class LogicalPlan;
}

namespace NES
{

class QueryManager
{
    // std::shared_ptr<const GRPCClient> grpcClient;

public:
    virtual ~QueryManager() = default;
    // explicit QueryManager(const std::shared_ptr<const GRPCClient>& grpcClient) : grpcClient(grpcClient) { }

    [[nodiscard]] virtual std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) noexcept = 0;
    virtual std::expected<void, Exception> start(QueryId queryId) noexcept = 0;
    virtual std::expected<void, Exception> stop(QueryId queryId) noexcept = 0;
    virtual std::expected<void, Exception> unregister(QueryId queryId) noexcept = 0;
    [[nodiscard]] virtual std::optional<QuerySummary> status(QueryId queryId) const = 0;
};
}
