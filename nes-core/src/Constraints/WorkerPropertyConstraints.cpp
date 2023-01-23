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

#include <Constraints/WorkerPropertyConstraints.hpp>
#include <Topology/TopologyNode.hpp>
#include <utility>

namespace NES::Constraint {

WorkerPropertyConstraints::WorkerPropertyConstraints(const std::vector<std::string>& propertiesToValidate)
    : propertiesToValidate(std::move(propertiesToValidate)) {}

bool WorkerPropertyConstraints::validate(const TopologyNodePtr& worker) const {

    for (const auto& property : propertiesToValidate) {
        if (!worker->hasNodeProperty(property)) {
            NES_WARNING2("Unable to find property {} in the worker with id {}", property, worker->getId());
            return false;
        }
    }
    return true;
}

}// namespace NES::Constraint