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
#include <cstddef>
#include <string>
#include <Configurations/BaseOption.hpp>

/// marker class for SequenceOption
class ISequenceOption : public NES::Configurations::BaseOption
{
public:
    ISequenceOption(const std::string& name, const std::string& description) : NES::Configurations::BaseOption(name, description) { }
    virtual BaseOption& operator[](size_t index) = 0;
    [[nodiscard]] virtual size_t size() const = 0;
};
