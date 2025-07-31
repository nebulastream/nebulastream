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

#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

class LegacyOptimizer
{
public:
    struct OptimizedLogicalPlan
    {
        LogicalPlan plan;
    };
    static OptimizedLogicalPlan
    optimize(LogicalPlan inputPlan, std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog);
};

}
