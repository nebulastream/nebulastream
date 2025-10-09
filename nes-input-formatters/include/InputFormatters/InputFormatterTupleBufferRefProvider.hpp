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

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

std::unique_ptr<InputFormatterTupleBufferRef> provideInputFormatterTupleBufferRef(
    const std::optional<ParserConfig>& formatScanConfig,
    std::shared_ptr<NES::Nautilus::Interface::BufferRef::TupleBufferRef> memoryProvider);

bool contains(const std::string& parserType);
}
