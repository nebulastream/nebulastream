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
#include <string>
#include <string_view>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <CompilationContext.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Config parameters for value serializers
struct ValueSerializerConfig
{
    bool quoted;
};

/// Base class for all value serializer implementations. A serializer traces its body into one nautilus function
/// per configuration, shared by every field that uses it, instead of inlining it at every column.
/// Unlike the deserializer side no common result type is needed: the function already returns a val<uint64_t>, and
/// each implementation knows the C++ type it serializes, so it extracts that from the VarVal (a trace-time variant
/// access, not a traced op) and hands a plain val<T> to its own function.
class ValueSerializer
{
public:
    virtual ~ValueSerializer() noexcept = default;

    /// Serialize the VarVal and write it, wrapped in prefix and delimiter, into the record buffer.
    /// Null handling, prefix and delimiter live inside the serializer so that a field is one call and one buffer
    /// write. At the call site they would need a traced branch on isNull, a second write, and would trace this
    /// body once per arm of that branch.
    /// 'prefix', 'nullLiteral' and 'delimiter' are trace-time constants per column that must outlive the pipeline.
    /// How a null renders belongs to the output format rather than the serializer -- CSV spells it 'NULL', JSON
    /// 'null', and both share the numeric serializers -- hence the parameter.
    /// 'isNull' is a trace-time constant false for a non-nullable field.
    [[nodiscard]] virtual nautilus::val<uint64_t> serializeAndWrite(
        CompilationContext& compilationContext,
        const VarVal& value,
        const nautilus::val<bool>& isNull,
        const nautilus::val<const char*>& prefix,
        const nautilus::val<const char*>& nullLiteral,
        const nautilus::val<const char*>& delimiter,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const
        = 0;

    /// Called once from the pipeline's setup(), so that the per-field path is a pointer compare rather than a name
    /// hash under a read lock. Each implementation memoizes its own function: unlike the deserializers, the proxy
    /// signature varies per serializer, so the memo cannot live here.
    virtual void resolve(CompilationContext& compilationContext) = 0;

protected:
    /// Identity of the traced function, composed once. Nautilus interns by name and silently collapses two bodies
    /// sharing one, so the name MUST encode every value the body bakes in -- notably quoting, which CSV takes from
    /// its config while JSON hardcodes it. Composed at construction because this is read per field per record.
    explicit ValueSerializer(const std::string_view tracedName)
        : tracedFunctionName(std::string{"ValueSerializer::"}.append(tracedName)) { }

    const std::string tracedFunctionName;
};
}
