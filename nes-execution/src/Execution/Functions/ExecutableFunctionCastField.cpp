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

#include <Execution/Functions/ExecutableFunctionCastField.hpp>

namespace NES::Runtime::Execution::Functions {

ExecutableFunctionCastField::ExecutableFunctionCastField(std::unique_ptr<Function> childFunction, std::shared_ptr<PhysicalType> castToType) : castToType(std::move(castToType)), childFunction(std::move(childFunction))
{
}

VarVal ExecutableFunctionCastField::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction->execute(record, arena);
    return value.castToType(castToType);
}
}
