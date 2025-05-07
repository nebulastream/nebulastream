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


#include <Sources/SourceReturnType.hpp>

namespace NES
{

class SourceImpl
{
public:
    virtual ~SourceImpl() = default;
    [[nodiscard]] virtual bool start(SourceReturnType::EmitFunction&& emitFunction) = 0;
    virtual void stop() = 0;
    [[nodiscard]] virtual SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) = 0;
    [[nodiscard]] virtual OriginId getOriginId() const = 0;
    [[nodiscard]] virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const SourceImpl& obj) { return obj.toString(os); }
};

}

FMT_OSTREAM(NES::SourceImpl);
