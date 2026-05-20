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
#include <utility>
#include <vector>

#include <Model.hpp>

namespace NES::detail
{

/// Friend-only factory for `Model<Tag>`. Keeps the public header free of the
/// internal `src/` classes that actually construct models.
struct ModelAccess
{
    static ImportedModel
    makeImported(RefCountedByteBuffer buf, std::string fnName, std::vector<size_t> inShape, std::vector<size_t> outShape)
    {
        return ImportedModel{
            ModelBackend::IREE, std::move(buf), RefCountedByteBuffer{}, std::move(fnName), std::move(inShape), std::move(outShape)};
    }

    static ImportedModel makeImported(
        RefCountedByteBuffer buf,
        RefCountedByteBuffer auxBuf,
        std::string fnName,
        std::vector<size_t> inShape,
        std::vector<size_t> outShape)
    {
        return ImportedModel{
            ModelBackend::OPENVINO, std::move(buf), std::move(auxBuf), std::move(fnName), std::move(inShape), std::move(outShape)};
    }

    static CompiledModel compileFrom(ImportedModel imported, RefCountedByteBuffer buf)
    {
        return CompiledModel{std::move(imported), std::move(buf)};
    }
};

}
