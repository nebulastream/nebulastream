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

#include <Operators/FaultTolerance/SNDeduplicationLogicalOperator.hpp>
#include <QueryOptimizer.hpp>

#include <Plans/LogicalPlan.hpp>
#include <DistributedLogicalPlan.hpp>

namespace NES
{

#include <uuid/uuid.h>
std::string generate_uuid() {
    uuid_t bin;
    uuid_generate_random(bin);
    char str[37];
    uuid_unparse_lower(bin, str);
    return std::string(str);
}

DistributedLogicalPlan QueryOptimizer::optimize(LogicalPlan plan) const
{
    auto root = plan.getRootOperators().at(0);
    std::cout << root << std::endl;
    auto uuid = generate_uuid();
    auto filePath = "/home/benni/Desktop/nebulastream/benni/dedup/" + uuid + ".txt";
    auto rootChild = SNDeduplicationLogicalOperator(WeakLogicalOperator{}, filePath).withChildren(root.getChildren());
    root = root.withChildren({rootChild});
    plan = plan.withRootOperators({root});
    plan = semanticAnalyzer.analyse(plan);
    plan = ruleBasedOptimization.optimize(plan);
    std::cout << explain(plan, ExplainVerbosity::Debug) << std::endl;
    return operatorPlacement.place(plan);
}

}
