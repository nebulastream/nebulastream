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

#include <expected>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>

namespace NES
{

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<LocalQueryId, Exception> registerQuery(const GrpcAddr& grpc, LogicalPlan) = 0;
    virtual std::expected<void, Exception> start(const LocalQuery&) = 0;
    virtual std::expected<void, Exception> stop(const LocalQuery&) = 0;
    virtual std::expected<void, Exception> unregister(const LocalQuery&) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatus, Exception> status(const LocalQuery&) const = 0;
};

class QueryManager
{
    UniquePtr<QuerySubmissionBackend> backend;

public:
    explicit QueryManager(UniquePtr<QuerySubmissionBackend> backend);
    [[nodiscard]] std::expected<Query, Exception> registerQuery(PlanStage::DistributedLogicalPlan&& plan);
    std::expected<void, Exception> start(const Query& query);
    std::expected<void, std::vector<Exception>> stop(const Query& query);
    std::expected<void, std::vector<Exception>> unregister(const Query& query);
    [[nodiscard]] std::expected<DistributedQueryStatus, std::vector<Exception>> status(const Query& query) const;
};

}
