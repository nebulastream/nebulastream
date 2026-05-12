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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <Util/Reflection.hpp>

namespace NES
{

enum class TensorElementType : uint8_t
{
    FLOAT32,
    UINT8,
    INT64,
};

[[nodiscard]] constexpr size_t tensorElementTypeSize(TensorElementType type)
{
    switch (type)
    {
        case TensorElementType::FLOAT32:
            return sizeof(float);
        case TensorElementType::UINT8:
            return sizeof(uint8_t);
        case TensorElementType::INT64:
            return sizeof(int64_t);
    }
    std::unreachable();
}

namespace detail
{

struct ModelAccess;

/// Ref-counted byte buffer shared by all `Model<Tag>` instances. Copying a
/// `Model` is cheap — it's just a shared_ptr bump.
struct RefCountedByteBuffer
{
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    std::shared_ptr<const std::byte[]> buffer;
    size_t size = 0;

    /// Allocate a new ref-counted byte buffer and copy `bytes` into it.
    static RefCountedByteBuffer fromBytes(std::span<const std::byte> bytes)
    {
        /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
        auto buf = std::make_shared<std::byte[]>(bytes.size());
        std::ranges::copy(bytes, buf.get());
        return {.buffer = std::move(buf), .size = bytes.size()};
    }

    friend bool operator==(const RefCountedByteBuffer& lhs, const RefCountedByteBuffer& rhs)
    {
        if (lhs.size != rhs.size)
        {
            return false;
        }
        if (!lhs.buffer || !rhs.buffer)
        {
            return lhs.buffer == rhs.buffer;
        }
        return std::ranges::equal(std::span{lhs.buffer.get(), lhs.size}, std::span{rhs.buffer.get(), rhs.size});
    }

    [[nodiscard]] std::span<const std::byte> view() const
    {
        if (!buffer)
        {
            return {};
        }
        return {buffer.get(), size};
    }
};

}

template <typename Tag>
class Model;

using ImportedModel = Model<struct Imported_>;
using CompiledModel = Model<struct Compiled_>;

/// A model at a particular lifecycle stage: textual form after import, compiled
/// bytecode after compile. The payload is a ref-counted byte buffer; the
/// signature (function name, shapes) scraped at import time flows unchanged
/// through compile.
template <typename Tag>
class Model
{
    detail::RefCountedByteBuffer data;
    std::string functionName;
    std::vector<size_t> inputShape;
    std::vector<size_t> outputShape;
    TensorElementType inputElementType;
    TensorElementType outputElementType;

    template <typename OtherTag>
    friend class Model;

    friend struct detail::ModelAccess;
    friend struct Reflector<Model<Imported_>>;
    friend struct Unreflector<Model<Imported_>>;

    Model(
        detail::RefCountedByteBuffer buf,
        std::string fnName,
        std::vector<size_t> inShape,
        std::vector<size_t> outShape,
        TensorElementType inType,
        TensorElementType outType)
        : data(std::move(buf))
        , functionName(std::move(fnName))
        , inputShape(std::move(inShape))
        , outputShape(std::move(outShape))
        , inputElementType(inType)
        , outputElementType(outType)
    {
    }

    /// Cross-tag conversion: carry the signature from `other` and take a fresh
    /// payload buffer — used when compiling turns one lifecycle stage into the next.
    template <typename OtherTag>
    Model(Model<OtherTag> other, detail::RefCountedByteBuffer buf)
        : data(std::move(buf))
        , functionName(std::move(other.functionName))
        , inputShape(std::move(other.inputShape))
        , outputShape(std::move(other.outputShape))
        , inputElementType(std::move(other.inputElementType))
        , outputElementType(std::move(other.outputElementType))
    {
    }

public:
    Model() = delete;
    Model(const Model&) = default;
    Model(Model&&) noexcept = default;
    Model& operator=(const Model&) = default;
    Model& operator=(Model&&) noexcept = default;
    ~Model() = default;

    [[nodiscard]] std::span<const std::byte> getData() const { return data.view(); }

    [[nodiscard]] size_t size() const { return data.size; }

    [[nodiscard]] bool empty() const { return data.size == 0; }

    [[nodiscard]] const std::string& getFunctionName() const { return functionName; }

    [[nodiscard]] const std::vector<size_t>& getInputShape() const { return inputShape; }

    [[nodiscard]] const std::vector<size_t>& getOutputShape() const { return outputShape; }

    [[nodiscard]] TensorElementType getInputElementType() const { return inputElementType; }

    [[nodiscard]] TensorElementType getOutputElementType() const { return outputElementType; }

    [[nodiscard]] size_t getNDim() const { return inputShape.size(); }

    [[nodiscard]] size_t getOutputDims() const { return outputShape.size(); }

    [[nodiscard]] size_t inputSize() const
    {
        return tensorElementTypeSize(inputElementType) * std::accumulate(inputShape.begin(), inputShape.end(), size_t{1}, std::multiplies<>());
    }

    [[nodiscard]] size_t outputSize() const
    {
        return tensorElementTypeSize(outputElementType) * std::accumulate(outputShape.begin(), outputShape.end(), size_t{1}, std::multiplies<>());
    }

    bool operator==(const Model&) const = default;
};

template <>
struct Reflector<ImportedModel>
{
    Reflected operator()(const ImportedModel& model) const;
};

template <>
struct Unreflector<ImportedModel>
{
    ImportedModel operator()(const Reflected& rfl) const;
};

}
