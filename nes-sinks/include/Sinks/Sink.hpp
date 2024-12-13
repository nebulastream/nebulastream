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

#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

class Sink
{
public:
    Sink(const QueryId queryId) : queryId(queryId) {};
    virtual ~Sink() = default;

    Sink(const Sink&) = delete;
    Sink& operator=(const Sink&) = delete;
    Sink(Sink&&) = delete;
    Sink& operator=(Sink&&) = delete;

    virtual bool emitTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) = 0;

    virtual void open() = 0;
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const Sink& sink);
    friend bool operator==(const Sink& lhs, const Sink& rhs);

    const QueryId queryId;

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
    [[nodiscard]] virtual bool equals(const Sink& other) const = 0;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::Sink> : ostream_formatter
{
};
}
