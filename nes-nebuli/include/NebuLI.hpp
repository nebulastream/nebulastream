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
#include <experimental/propagate_const>
#include <GRPCClient.hpp>

namespace NES::CLI
{

class Nebuli
{
    std::experimental::propagate_const<std::shared_ptr<GRPCClient>> grpcClient;

public:
    explicit Nebuli(const std::shared_ptr<GRPCClient>& grpcClient) : grpcClient(grpcClient) { }

    QueryId registerQuery(const LogicalPlan& plan);
    void startQuery(QueryId queryId);
    void stopQuery(QueryId queryId);
    void unregisterQuery(QueryId queryId);
};
}
