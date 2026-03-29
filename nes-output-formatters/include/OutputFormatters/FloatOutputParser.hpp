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

#include <string>

namespace NES
{

/// Base class for all float output parser implementations, which take a float or a double and convert it into a string
class FloatOutputParser
{
public:
    FloatOutputParser() noexcept = default;
    virtual ~FloatOutputParser() noexcept = default;

    virtual void floatToString(const float& value, std::string& output) const = 0;
    virtual void floatToString(const double& value, std::string& output) const = 0;
};
}
