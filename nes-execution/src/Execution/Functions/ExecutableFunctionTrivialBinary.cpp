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
#include <utility>
#include <vector>
#include <Util/Execution.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Functions
{

#define BinaryFunction(name, operator) \
    class ExecutableFunction##name final : public Function \
    { \
    public: \
        ExecutableFunction##name( \
            std::unique_ptr<Function> leftExecutableFunctionSub, std::unique_ptr<Function> rightExecutableFunctionSub) \
            : leftExecutableFunctionSub(std::move(leftExecutableFunctionSub)) \
            , rightExecutableFunctionSub(std::move(rightExecutableFunctionSub)) \
        { \
        } \
        VarVal execute(Record& record) const override \
        { \
            const auto leftValue = leftExecutableFunctionSub->execute(record); \
            const auto rightValue = rightExecutableFunctionSub->execute(record); \
            return leftValue operator rightValue; \
        } \
\
    private: \
        const std::unique_ptr<Function> leftExecutableFunctionSub; \
        const std::unique_ptr<Function> rightExecutableFunctionSub; \
    }; \
    std::unique_ptr<Function> RegisterExecutableFunction##name(std::vector<std::unique_ptr<Functions::Function>> childFunctions) \
    { \
        PRECONDITION(childFunctions.size() == 2, #name " function must have exactly two sub-functions"); \
        return std::make_unique<ExecutableFunction##name>(std::move(childFunctions[0]), std::move(childFunctions[1])); \
    }

BinaryFunction(Equals, ==);
BinaryFunction(NotEquals, !=);
BinaryFunction(Less, <);
BinaryFunction(LessEquals, <=);
BinaryFunction(GreaterEquals, >=);
BinaryFunction(Greater, >);
BinaryFunction(Add, +);
BinaryFunction(Sub, -);
BinaryFunction(Mul, *);
BinaryFunction(Div, /);
}
