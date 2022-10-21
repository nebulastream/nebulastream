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
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
#include <Execution/Operators/Operator.hpp>
namespace NES::Runtime::Execution {

class PhysicalOperatorPipeline {
  public:
    Operators::Operator* getRootOperator() const;
    void setRootOperator(Operators::Operator* rootOperator);

  private:
    Operators::Operator* rootOperator;
};

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PHYSICALOPERATORPIPELINE_HPP_
