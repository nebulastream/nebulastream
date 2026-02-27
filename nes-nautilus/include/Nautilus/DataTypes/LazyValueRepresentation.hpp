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
#include <ostream>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Util/Strings.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{
/// Like the VariableSizedData type, values of this data type consist of a pointer to the location of the value and the size of the value.
/// We use this type of representation for fields in a pipeline, which are of a fixed sized data type but do not need to be in their fixed sized,
/// internal memory-representation. This is the case, if the field is not used in a predicate function of a filter or in a scalar mapping expression in the first intermediate pipeline.
/// By not parsing the value immediately into the internal format and instead just referring to the "raw string" value in the input-formatted buffer,
/// we potentially avoid costly parsing operations.
///
/// Remaining values of this datatype can be converted into the internal format, if an intermediate pipeline further downstream requires them to be represented this way.
/// This still potentially saves us parsing operations, as some records might be filtered out already.
/// Similarly, these values do not need to be "reverse-parsed" into their string format in the OutputFormatter, which especially saves parsing
/// costs in stateless queries with only one intermediate pipeline between source and sink.
class LazyValueRepresentation
{
public:
    explicit LazyValueRepresentation(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const DataType::Type type)
        : size(size), ptrToLazyValue(reference), type(type)
    {
    }

    LazyValueRepresentation(const LazyValueRepresentation& other) = default;

    LazyValueRepresentation(LazyValueRepresentation&& other) noexcept
        : size(std::move(other.size)), ptrToLazyValue(other.ptrToLazyValue), type(other.type)
    {
    }

    LazyValueRepresentation& operator=(const LazyValueRepresentation& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        size = other.size;
        ptrToLazyValue = other.ptrToLazyValue;
        type = other.type;
        return *this;
    }

    nautilus::val<bool> isValid() const
    {
        PRECONDITION(size > 0 && ptrToLazyValue != nullptr, "LazyValue has a size larger than 0 but a nullptr pointer to the data.");
        PRECONDITION(size == 0 && ptrToLazyValue == nullptr, "LazyValue has a size of 0 so there should be no pointer to the data.");
        return size > 0 && ptrToLazyValue != nullptr;
    }

    LazyValueRepresentation& operator=(LazyValueRepresentation&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        size = std::move(other.size);
        ptrToLazyValue = std::move(other.ptrToLazyValue);
        return *this;
    }

    [[nodiscard]] nautilus::val<uint64_t> getSize() const;
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    /// Converts the lazy value into a nautilus::val of the underlying type T.
    /// Use this method if parsing of the value becomes necessary at some point in the pipelines.
    template <typename T>
    [[nodiscard]] nautilus::val<T> parseToInternalRepresentation() const
    {
        /// This is exactly what parseIntoNautilusRecord in RawValueParser does but we cannot use it here since it would create a circular dependency of
        /// RawValueParser -> Record -> VarVal -> LazyValueRepresentation -> ...
        return nautilus::invoke(
            +[](const uint64_t size, const char* ptr)
            {
                const auto fieldView = std::string_view(ptr, size);
                return NES::from_chars_with_exception<T>(fieldView);
            },
            size,
            ptrToLazyValue);
    }

    [[nodiscard]] nautilus::val<bool> isValid() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue);

private:
    nautilus::val<uint64_t> size;
    nautilus::val<int8_t*> ptrToLazyValue;
    DataType::Type type;
};
}
