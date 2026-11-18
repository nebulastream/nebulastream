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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
class ModelCatalog;

/// A cluster of workers with sources and sinks for tests of the distributed planning. Queries are given as SQL and optimized with the
/// default optimizer configuration. All fields are UINT64.
class DistributedTestCluster
{
public:
    DistributedTestCluster();

    /// Adds a worker with the given downstream workers. The data address is the host.
    void addWorker(const std::string& host, const std::vector<std::string>& downstream);
    void addWorker(const std::string& host, const std::vector<std::string>& downstream, size_t capacity);

    /// Adds a logical source and a physical source on the given host.
    void addSource(const std::string& name, const std::vector<std::string>& fields, const std::string& host);

    /// Adds a sink on the given host.
    void addSink(const std::string& name, const std::vector<std::string>& fields, const std::string& host);

    [[nodiscard]] DistributedLogicalPlan optimize(const std::string& sql) const;

    SharedPtr<WorkerCatalog> workerCatalog;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
    std::shared_ptr<ModelCatalog> modelCatalog;
};

}
