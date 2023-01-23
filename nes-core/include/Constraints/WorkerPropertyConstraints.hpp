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

#ifndef NES_WORKERPROPERTYCONSTRAINTS_HPP
#define NES_WORKERPROPERTYCONSTRAINTS_HPP

#include <Constraints/WorkerConstraints.hpp>
#include <string>
#include <vector>

namespace NES::Constraint {

/**
 * constraint that checks if a worker node has specific property or not
 */
class WorkerPropertyConstraints : public WorkerConstraints {
  public:
    WorkerPropertyConstraints(const std::vector<std::string>& propertiesToValidate);

    ~WorkerPropertyConstraints() = default;

    bool validate(const TopologyNodePtr& worker) const override;

  private:
    std::vector<std::string> propertiesToValidate;
};

}// namespace NES::Constraint
#endif//NES_WORKERPROPERTYCONSTRAINTS_HPP
