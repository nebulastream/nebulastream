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

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES
{

class AsyncSource
{
public:
    using AsyncSourceEmit = std::function<void(SourceReturnType::SourceReturnType)>;
    AsyncSource() = default;
    virtual ~AsyncSource() = default;

    virtual void open(std::shared_ptr<AbstractBufferProvider>, AsyncSourceEmit&& emitFunction) = 0;
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const AsyncSource& source);

protected:
    /// Implemented by children of AsyncSource. Called by '<<'. Allows to use '<<' on abstract AsyncSource.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

inline std::ostream& operator<<(std::ostream& out, const AsyncSource& source)
{
    return source.toString(out);
}

}

FMT_OSTREAM(NES::AsyncSource);
