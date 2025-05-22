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
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution
{

/// The physical operator pipeline captures a set of operators within a pipeline.
class PhysicalOperatorPipeline
{
public:
    void setRootOperator(std::shared_ptr<Operators::Operator> rootOperator);
    [[nodiscard]] std::shared_ptr<Operators::Operator> getRootOperator() const;

private:
    std::shared_ptr<Operators::Operator> rootOperator;
};

}
