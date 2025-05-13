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
#include <Identifiers/Identifiers.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <GRPCClient.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
class QueryPlan;
}
namespace NES::Catalogs::Source
{
class SourceCatalog;
}
namespace NES::CLI
{

class Optimizer
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

public:
    std::shared_ptr<DecomposedQueryPlan> optimize(std::shared_ptr<QueryPlan>& plan);
    Optimizer() = default;
    explicit Optimizer(const std::shared_ptr<NES::Catalogs::Source::SourceCatalog>& sourceCatalog) : sourceCatalog(sourceCatalog) { }
};
class Nebuli
{
    std::shared_ptr<const GRPCClient> grpcClient;

public:
    explicit Nebuli(const std::shared_ptr<const GRPCClient>& grpcClient) : grpcClient(grpcClient) { }

    QueryId registerQuery(const std::shared_ptr<DecomposedQueryPlan>& plan);
    void startQuery(QueryId queryId);
    void stopQuery(QueryId queryId);
    void unregisterQuery(QueryId queryId);
};
}
