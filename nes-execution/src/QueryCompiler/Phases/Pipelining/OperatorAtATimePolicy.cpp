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
#include <memory>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorAtATimePolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>

namespace NES::QueryCompilation
{
bool OperatorAtATimePolicy::isFusible(std::shared_ptr<PhysicalOperators::PhysicalOperator>)
{
    return false;
}

std::shared_ptr<OperatorFusionPolicy> OperatorAtATimePolicy::create()
{
    return std::make_shared<OperatorAtATimePolicy>();
}
}
