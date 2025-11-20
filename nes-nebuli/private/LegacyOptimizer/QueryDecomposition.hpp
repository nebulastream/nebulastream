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

#include <Identifiers/NESStrongType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedQuery.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

using ChannelId = NESStrongUUIDType<struct ChannelId_>;

class QueryDecomposer
{
    SharedPtr<const WorkerCatalog> workerCatalog;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;

public:
    QueryDecomposer(
        SharedPtr<const WorkerCatalog> workerCatalog,
        SharedPtr<const SourceCatalog> sourceCatalog,
        SharedPtr<const SinkCatalog> sinkCatalog);

    DistributedLogicalPlan decompose(const LogicalPlan& placedPlan);
};

}
