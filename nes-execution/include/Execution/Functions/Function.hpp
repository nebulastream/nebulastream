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
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Nautilus
{
class Record;
using RecordPtr = std::shared_ptr<Record>;
} /// namespace NES::Nautilus

namespace NES::Runtime::Execution::Functions
{
using namespace Nautilus;
class Any;
class Function;
using FunctionPtr = std::shared_ptr<Function>;

/**
 * @brief Base class for all functions.
 */
class Function
{
public:
    /**
     * @brief Evaluates the functions on a record.
     * @param record
     * @return VarVal
     */
    virtual VarVal execute(Record& record) const = 0;
    virtual ~Function() = default;
};

} /// namespace NES::Runtime::Execution::Functions
