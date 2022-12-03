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

#ifndef NES_BOOLCONSTRAINT_HPP
#define NES_BOOLCONSTRAINT_HPP

#include <Constraints/Constraint.hpp>
#include <string>

namespace NES::Constraint {

/**
 * Constraint that requires a worker with desired configuration. The configuration key is passes as the constructor parameter.
 */
class NeedWorkerWithConfiguredConstraint : public Constraint {
    NeedWorkerWithConfiguredConstraint(std::string configurationKey);

  private:
    std::string configurationKey;
};

}// namespace NES::Constraint

#endif//NES_BOOLCONSTRAINT_HPP
