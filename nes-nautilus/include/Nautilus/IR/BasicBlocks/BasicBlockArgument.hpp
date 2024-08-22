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

#include <ostream>
#include <string>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <__fwd/ostream.h>

namespace NES::Nautilus::IR::Operations
{

class BasicBlockArgument : public Operation
{
public:
    explicit BasicBlockArgument(const std::string identifier, Types::StampPtr stamp);
    ~BasicBlockArgument() override = default;
    friend std::ostream& operator<<(std::ostream& os, const BasicBlockArgument& argument);
    std::string toString() override;
};

} /// namespace NES::Nautilus::IR::Operations
