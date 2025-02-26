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

#include <memory>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/BlockingSource.hpp>

namespace NES::Sources
{

template <typename SourceType>
struct SourceExecutionContext
{
    const OriginId originId;
    std::unique_ptr<SourceType> sourceImpl;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter;
};

}
