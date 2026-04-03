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

#include <Nautilus/DataTypes/VarVal.hpp>
#include <val_bool.hpp>

namespace NES
{
/// Base class for all input parsers
class InputParser
{
public:
    InputParser() noexcept = default;
    virtual ~InputParser() noexcept = default;

    /// Parse the value from string to it's c++ type representation
    /// Also performs null handling
    virtual VarVal parseToVarVal(
        bool nullable,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const = 0;

    /// For this value, nullable and null are already known and do not need to be searched for again
    virtual VarVal parseLazyToVarVal(
        const bool& nullable,
        const nautilus::val<bool>& isNull,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize) const = 0;
};
}
