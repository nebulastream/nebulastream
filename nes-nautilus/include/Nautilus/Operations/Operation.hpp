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
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>

namespace NES::Operations {

using TupleBuffer = Memory::TupleBuffer;

template<typename StateType>
class Operation {
public:
    virtual ~Operation() = default;
    virtual void execute(StateType& state, const TupleBuffer& input) = 0;
    virtual State::Operation describe() const = 0;
};

}
