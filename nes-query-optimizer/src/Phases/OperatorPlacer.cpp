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

#include <Phases/OperatorPlacer.hpp>

#include <Placement/BottomUpPlacement.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
DistributedLogicalPlan OperatorPlacer::place(LogicalPlan plan) const
{
    /// The placement machinery only considers the first root; a multi-root (fan-out) plan would silently
    /// lose its remaining sinks. The rule pipeline before placement handles DAGs, placement does not yet.
    PRECONDITION(
        plan.getRootOperators().size() == 1, "Operator placement only supports single-root plans (multi-sink placement is unsupported)");
    BottomUpOperatorPlacer(copyPtr(workerCatalog)).apply(plan);

    return QueryDecomposer(copyPtr(workerCatalog), copyPtr(sourceCatalog), copyPtr(sinkCatalog))
        .decompose(plan, defaultQueryOptimization.network);
}
}
