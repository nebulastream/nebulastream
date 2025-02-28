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

#include <cstdint>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>
#include <variant>
#include <PipelinedQueryPlan.hpp>
#include "API/Schema.hpp"
#include <Abstract/PhysicalOperator.hpp>
#include "DefaultEmitPhysicalOperator.hpp"
#include "DefaultScanPhysicalOperator.hpp"
#include "ErrorHandling.hpp"
#include "MemoryLayout/RowLayout.hpp"
#include "Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp"
#include "Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Plans/Operator.hpp"
#include "Plans/QueryPlan.hpp"

namespace NES::QueryCompilation::PipeliningPhase
{
/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj
PipelinedQueryPlan apply(const QueryPlan& queryPlan);
}
