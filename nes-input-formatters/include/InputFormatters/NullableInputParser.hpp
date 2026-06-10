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
#include <InputParser.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES
{
/// Class for input parsers, which should handle null-values. It extends the InputParser base class by adding a function that creates a VarVal
/// representing a NULL value of the corresponding datatype.
class NullableInputParser : public InputParser
{
public:
    NullableInputParser() noexcept = default;

    /// Return the Null-Representation of a datatype
    [[nodiscard]] virtual VarVal writeNull() const = 0;
};
}