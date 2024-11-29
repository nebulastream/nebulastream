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

#include <cstdint>
#include <iostream>
#include <memory>
#include <variant>

#include "boost/asio/awaitable.hpp"

#include "Identifiers/Identifiers.hpp"
#include "Runtime/AbstractBufferProvider.hpp"
#include "Sources/Source.hpp"
#include "Sources/SourceReturnType.hpp"


namespace NES::Sources
{

namespace asio = boost::asio;

using BlockingType = std::monostate;
using CoroutineType = asio::awaitable<void>;
using SourceType = std::variant<BlockingType, CoroutineType>;

class SourceRunner
{
public:
    explicit SourceRunner(
        OriginId originId,
        std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
        SourceReturnType::EmitFunction&& emitFn,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImpl)
        : originId(originId)
        , poolProvider(std::move(poolProvider))
        , emitFn(std::move(emitFn))
        , numSourceLocalBuffers(numSourceLocalBuffers)
        , sourceImpl(std::move(sourceImpl))
    {
    }

    SourceRunner() = delete;
    virtual ~SourceRunner() = default;

    SourceRunner(const SourceRunner&) = delete;
    SourceRunner& operator=(const SourceRunner&) = delete;
    SourceRunner(SourceRunner&&) = delete;
    SourceRunner& operator=(SourceRunner&&) = delete;


    virtual void close() = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual SourceType run() = 0;


    [[nodiscard]] OriginId getOriginId() const { return originId; }


protected:
    const OriginId originId;
    std::shared_ptr<NES::Memory::AbstractPoolProvider> poolProvider;
    SourceReturnType::EmitFunction emitFn;
    std::shared_ptr<NES::Memory::AbstractBufferProvider> bufferProvider{nullptr};
    uint64_t numSourceLocalBuffers;
    uint64_t maxSequenceNumber = 0;
    std::unique_ptr<Source> sourceImpl;


    friend std::ostream& operator<<(std::ostream& out, const SourceRunner& sourceRunner)
    {
        out << "\nSourceRunner(";
        out << "\n  originId: " << sourceRunner.originId;
        out << "\n  numSourceLocalBuffers: " << sourceRunner.numSourceLocalBuffers;
        out << "\n  maxSequenceNumber: " << sourceRunner.maxSequenceNumber;
        out << "\n  source implementation:" << *sourceRunner.sourceImpl;
        out << ")\n";
        return out;
    }
};

}
