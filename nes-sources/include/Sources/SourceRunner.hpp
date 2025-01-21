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

#include <Sources/SourceUtility.hpp>

namespace NES::Sources
{

class SourceRunner
{
public:
    SourceRunner() = delete;
    explicit SourceRunner(const OriginId originId) : originId(originId) { }
    virtual ~SourceRunner() = default;

    virtual bool start(EmitFunction&& emitFn) = 0;
    virtual bool stop() = 0;

    virtual OriginId getSourceId() { return originId; }

    friend std::ostream& operator<<(std::ostream& out, const SourceRunner& sourceRunner) { return sourceRunner.toString(out); }

protected:
    /// Implemented by children of SourceRunner. Called by '<<'. Allows to use '<<' on abstract SourceRunner.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;

    const OriginId originId;
};

}
