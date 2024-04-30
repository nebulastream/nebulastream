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

#include <StatisticCollection/ProbeQueryGenerator/DefaultProbeQueryGenerator.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/Probing/PhysicalCountMinProbeOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {
DecomposedQueryPlanPtr DefaultProbeQueryGenerator::generateProbeQuery(const StatisticProbeRequest&,
                                                                      const Statistic& statistic) {
    // As we are merely creating an ad-hoc query, we do not have to worry about the query plan ID, shared query ID, or worker node ID.
    auto decomposableQueryPlan = DecomposedQueryPlan::create(INVALID_DECOMPOSED_QUERY_PLAN_ID, INVALID_SHARED_QUERY_ID, INVALID_WORKER_NODE_ID);

    if (statistic.instanceOf<CountMinStatistic>()) {
//        decomposableQueryPlan->addRootOperator();
    } else {
        NES_NOT_IMPLEMENTED();
    }

    NES_NOT_IMPLEMENTED();
}
}// namespace NES::Statistic