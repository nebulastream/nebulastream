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
#include <functional>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Util/Reflection.hpp>

namespace NES {
    namespace detail {
        struct ModelAccess;

        /// Ref-counted byte buffer shared by all `Model<Tag>` instances. Copying a
        /// `Model` is cheap — it's just a shared_ptr bump.
        struct RefCountedByteBuffer {
            /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
            std::shared_ptr<const std::byte[]> buffer;
            size_t size = 0;

            /// Allocate a new ref-counted byte buffer and copy `bytes` into it.
            static RefCountedByteBuffer fromBytes(std::span<const std::byte> bytes) {
                /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
                auto buf = std::make_shared<std::byte[]>(bytes.size());
                std::ranges::copy(bytes, buf.get());
                return {.buffer = std::move(buf), .size = bytes.size()};
            }

            friend bool operator==(const RefCountedByteBuffer &lhs, const RefCountedByteBuffer &rhs) {
                if (lhs.size != rhs.size) {
                    return false;
                }
                if (!lhs.buffer || !rhs.buffer) {
                    return lhs.buffer == rhs.buffer;
                }
                return std::ranges::equal(std::span{lhs.buffer.get(), lhs.size}, std::span{rhs.buffer.get(), rhs.size});
            }

            [[nodiscard]] std::span<const std::byte> view() const {
                if (!buffer) {
                    return {};
                }
                return {buffer.get(), size};
            }
        };
    }

    template<typename Tag>
    class Model;

    using ImportedModel = Model<struct Imported_>;
    using CompiledModel = Model<struct Compiled_>;

    /// A model at a particular lifecycle stage: textual form after import, compiled
    /// bytecode after compile. The payload is a ref-counted byte buffer; the
    /// signature (function name, shapes) scraped at import time flows unchanged
    /// through compile.
    template<typename Tag>
    class Model {
        detail::RefCountedByteBuffer data;
        std::string functionName;
        std::vector<size_t> inputShape;
        std::vector<size_t> outputShape;

        template<typename OtherTag>
        friend class Model;

        friend struct detail::ModelAccess;
        friend struct Reflector<Model<Imported_> >;
        friend struct Unreflector<Model<Imported_> >;

        Model(detail::RefCountedByteBuffer buf, std::string fnName, std::vector<size_t> inShape,
              std::vector<size_t> outShape)
            : data(std::move(buf)), functionName(std::move(fnName)), inputShape(std::move(inShape)),
              outputShape(std::move(outShape)) {
        }

        /// Cross-tag conversion: carry the signature from `other` and take a fresh
        /// payload buffer — used when compiling turns one lifecycle stage into the next.
        template<typename OtherTag>
        Model(Model<OtherTag> other, detail::RefCountedByteBuffer buf)
            : data(std::move(buf))
              , functionName(std::move(other.functionName))
              , inputShape(std::move(other.inputShape))
              , outputShape(std::move(other.outputShape)) {
        }

    public:
        Model() = delete;

        Model(const Model &) = default;

        Model(Model &&) noexcept = default;

        Model &operator=(const Model &) = default;

        Model &operator=(Model &&) noexcept = default;

        ~Model() = default;

        [[nodiscard]] std::span<const std::byte> getData() const { return data.view(); }

        [[nodiscard]] size_t size() const { return data.size; }

        [[nodiscard]] bool empty() const { return data.size == 0; }

        [[nodiscard]] const std::string &getFunctionName() const { return functionName; }

        [[nodiscard]] const std::vector<size_t> &getInputShape() const { return inputShape; }

        [[nodiscard]] const std::vector<size_t> &getOutputShape() const { return outputShape; }

        [[nodiscard]] size_t getNDim() const { return inputShape.size(); }

        [[nodiscard]] size_t getOutputDims() const { return outputShape.size(); }

        [[nodiscard]] size_t inputSize() const {
            return sizeof(float) *
                   std::accumulate(inputShape.begin(), inputShape.end(), size_t{1}, std::multiplies<>());
        }

        [[nodiscard]] size_t outputSize() const {
            return sizeof(float) * std::accumulate(outputShape.begin(), outputShape.end(), size_t{1},
                                                   std::multiplies<>());
        }

        bool operator==(const Model &) const = default;
    };

    template<>
    struct Reflector<ImportedModel> {
        Reflected operator()(const ImportedModel &model) const;
    };

    template<>
    struct Unreflector<ImportedModel> {
        ImportedModel operator()(const Reflected &rfl, const ReflectionContext &context) const;
    };

    /// User-declared input and output field schemas of a model. Not a property of
    /// the MLIR/bytecode — it's catalog-side metadata, declared alongside the model
    /// in `CREATE MODEL` and passed through to the logical operator for schema
    /// inference.
    struct
            ModelSchema /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding Schema (vector) trip the check; no real escape
    {
        /// Ordered because the runtime indexes inputs/outputs positionally by tensor slot.
        Schema<UnqualifiedUnboundField, Ordered> inputs;
        Schema<UnqualifiedUnboundField, Ordered> outputs;

        bool operator==(const ModelSchema &) const = default;
    };

    /// A catalog entry: the user-given name and source path together with the
    /// imported model and the validated schema.
    ///
    /// Constructible only through `ModelCatalog::registerModel` (which validates)
    /// or through reflection (which trusts the coordinator-side checks). Callers
    /// must route through those paths — there is no public constructor.
    class RegisteredModel {
        std::string name;
        std::filesystem::path path;
        ImportedModel imported;
        ModelSchema schema;

        RegisteredModel(std::string name, std::filesystem::path path, ImportedModel importedModel,
                        ModelSchema modelSchema)
            : name(std::move(name)), path(std::move(path)), imported(std::move(importedModel)),
              schema(std::move(modelSchema)) {
        }

        friend struct Reflector<RegisteredModel>;
        friend struct Unreflector<RegisteredModel>;

    public:
        static RegisteredModel create(std::string name, std::filesystem::path path, ModelSchema schema);

        [[nodiscard]] const std::string &getName() const { return name; }

        [[nodiscard]] const std::filesystem::path &getPath() const { return path; }

        [[nodiscard]] const ImportedModel &getImported() const { return imported; }

        [[nodiscard]] const ModelSchema &getSchema() const { return schema; }

        bool operator==(const RegisteredModel &) const = default;
    };

    template<>
    struct Reflector<RegisteredModel> {
        Reflected operator()(const RegisteredModel &model) const;
    };

    template<>
    struct Unreflector<RegisteredModel> {
        RegisteredModel operator()(const Reflected &rfl, const ReflectionContext &context) const;
    };
}
