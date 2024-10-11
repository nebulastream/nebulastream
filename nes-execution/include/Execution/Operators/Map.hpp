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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief Map operator that evaluates a map function on a input records.
 * Map functions read record fields, apply transformations, and can set/update fields.
 */
class Map : public ExecutableOperator
{
public:
    /**
     * @brief Creates a map operator with a map function.
     * @param mapFunction map function.
     */
    Map(std::unique_ptr<Functions::Function> mapFunction) : mapFunction(std::move(mapFunction)) {};
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    const std::unique_ptr<Runtime::Execution::Functions::Function> mapFunction;
};

}
