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

#include <RawValueParser.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/LazyValueProvider.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{

bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

VarVal parseFixedSizeIntoVarVal(
    const bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::string& parserType)

{
    constexpr InputParserRegistryArguments args{};
    if (const auto parser = InputParserRegistry::instance().create(parserType, args))
    {
        return parser.value()->parseToVarVal(nullable, fieldAddress, fieldSize, nullValues);
    }
    throw UnknownInputParserType("Unknown Input Parser: {}", parserType);
}

namespace
{
/// ABLATION KNOB (env NES_EAGER_COPY_VARSIZED, effective only with INPUT_FORMATTER.eager_parse = true): the uncompiled
/// baseline rung materialises every VARSIZED field by copying its bytes into a child buffer, whereas a compiled
/// eager "parse" of a VARSIZED field is a (pointer, size) view that LLVM drops when the field is unused. For a
/// work-equal "compiled parse-all" rung this proxy copies the field bytes into a thread-local ring arena. It is an
/// opaque side-effecting call, so the JIT must keep it. Benchmark-only; unset = unchanged engine behaviour.
void eagerCopyVarsizedProxy(const int8_t* src, const uint64_t size) noexcept
{
    constexpr size_t arenaBytes = size_t{1} << 20;
    thread_local std::vector<int8_t> arena(arenaBytes);
    thread_local size_t offset = 0;
    if (src == nullptr || size == 0)
    {
        return;
    }
    const size_t bytes = std::min<size_t>(size, arenaBytes);
    if (offset + bytes > arenaBytes)
    {
        offset = 0;
    }
    std::memcpy(arena.data() + offset, src, bytes);
    offset += bytes;
}
}

void parseRawValueIntoRecord(
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    const QuotationType quotationType,
    const std::string& parserType,
    const bool& lazyOverload,
    const Characteristic varSizedCharacteristics,
    const bool eagerParse)
{
    /// Raw values will be transformed into a lazy representation of pointer, size and underlying datatype instead of being parsed immediatly.
    /// Should a field be needed in its parsed form for the execution of a logical function, it will be parsed in the overridden function in LazyValueRepresentation
    /// For this, we pass the parser type string into the members of the lazy value representation
    if (dataType.type == DataType::Type::UNDEFINED)
    {
        throw NotImplemented("Cannot parse undefined type.");
    }
    nautilus::val<bool> isNull = false;
    if (dataType.nullable)
    {
        /// For lazy values, it is pivotal to know, if the underlying val is null, in case we want to perform overridden logical functions with the lazy vals
        isNull = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
            checkIsNullProxy,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
    }
    /// EAGER_PARSE (hoisting-study arm): the lazy representation is built exactly as below, but we materialise it
    /// IMMEDIATELY (parseValue()) and write the concrete value into the record, so the parse/view is emitted at
    /// scan time -- upstream of any downstream selection branch -- and the record carries NO lazy values (the
    /// "resolve * first, then filter" shape). Materialising every leaf (not just the numerics) keeps the record
    /// homogeneous: a mixed lazy/materialised record breaks the output passthrough fast path (varsized fields
    /// serialise as NULL in the fused pipeline). Eager varsized loses its passthrough Characteristics (it takes
    /// the escaping path), which is the point -- eager is the un-optimised baseline. The default (false) keeps
    /// the lazy path, which defers each parse to its use site inside the taken filter branch.
    const auto writeFieldValue = [&](const std::shared_ptr<LazyValueRepresentation>& lazyVal)
    {
        if (eagerParse)
        {
            record.write(fieldName, lazyVal->parseValue());
        }
        else
        {
            record.write(fieldName, {lazyVal, dataType.nullable, isNull});
        }
    };
    if (dataType.type == DataType::Type::CHAR && quotationType == QuotationType::DOUBLE_QUOTE)
    {
        /// Char does not have overloads for lazy parses currently
        const auto lazyVal = LazyValueProvider::provideLazyValueRepresentation(
            std::string(magic_enum::enum_name(dataType.type)),
            fieldAddress + nautilus::val<uint32_t>(1),
            fieldSize - nautilus::val<uint32_t>(2),
            dataType,
            isNull,
            parserType);
        writeFieldValue(lazyVal);
        return;
    }
    if (dataType.type == DataType::Type::VARSIZED)
    {
        /// Ablation knob, see eagerCopyVarsizedProxy. Read once; host-side (trace-time) code, not spliced into the JIT.
        static const bool eagerCopyVarsized = std::getenv("NES_EAGER_COPY_VARSIZED") != nullptr;
        /// Varsized Lazy Values are basically VariableSized Data objects. Therefore, there is no harm in always using the overloaded logical functions for this type
        switch (quotationType)
        {
            case QuotationType::NONE: {
                const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress);
                const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize);
                const auto lazyVal = LazyValueProvider::provideLazyValueRepresentation(
                    std::string(magic_enum::enum_name(dataType.type)), ptr, size, dataType, isNull, parserType);
                /// Byte-property characteristics asserted by the input formatter (see Characteristic /
                /// varSizedCharacteristicsOf). Unquoted CSV text carries CLEAN|JSON_ESCAPED when its source
                /// asserts `assume_clean_strings` (default) -- turning on csv->json string passthrough -- or NONE
                /// when opted down (a source with real `"`/`\`, cf. formatter/SpecialCharactersJSON), which keeps
                /// the safe escaping path. Native raw text passes NONE.
                lazyVal->setCharacteristics(varSizedCharacteristics);
                writeFieldValue(lazyVal);
                if (eagerParse && eagerCopyVarsized)
                {
                    nautilus::invoke(eagerCopyVarsizedProxy, ptr, size);
                }
                return;
            }
            case QuotationType::DOUBLE_QUOTE: {
                const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress + nautilus::val<uint32_t>(1));
                const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize - nautilus::val<uint64_t>(2));
                const auto lazyVal = LazyValueProvider::provideLazyValueRepresentation(
                    std::string(magic_enum::enum_name(dataType.type)), ptr, size, dataType, isNull, parserType);
                /// JSON string body (the bytes between the quotes, e.g. from SchemaJSON's raw-span path): the
                /// caller passes JSON_ESCAPED (varSizedCharacteristicsOf's DOUBLE_QUOTE default) -- already a
                /// valid JSON string body, so it forwards VERBATIM into a JSON string field with NO per-row
                /// escape scan (the json->json string passthrough win).
                lazyVal->setCharacteristics(varSizedCharacteristics);
                writeFieldValue(lazyVal);
                if (eagerParse && eagerCopyVarsized)
                {
                    nautilus::invoke(eagerCopyVarsizedProxy, ptr, size);
                }
                return;
            }
        }
        std::unreachable();
    }
    /// Check if the overloaded logical functions on this type are desired.
    const std::string lazyValName = lazyOverload ? std::string(magic_enum::enum_name(dataType.type)) : "";
    const auto lazyVal
        = LazyValueProvider::provideLazyValueRepresentation(lazyValName, fieldAddress, fieldSize, dataType, isNull, parserType);
    writeFieldValue(lazyVal);
}

VarVal parseLazyIntoVarVal(
    const bool nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& parserType)
{
    /// A null field address means a projected field was MISSING at extraction: the JSON numeric extractor
    /// (getRawJsonWithNullCheck) soft-fails to a {nullptr,0} sentinel instead of throwing, so that its invoke
    /// can be marked noUnwind and LLVM can DCE the extraction of UNUSED fields (effective projection pushdown).
    /// Materialisation is reached ONLY for USED fields, so a used missing field errors here with FieldNotFound
    /// (ERROR 2004) -- unused missing fields are eliminated and never reach this point. CSV/native formatters
    /// always pass a valid span, so this is an inert, well-predicted branch for them.
    /// MICROBENCH KNOB (NES_ASSUME_VALID_SPANS): CSV/native formatters always pass a valid span, so this
    /// sentinel is a dead branch for them -- but LLVM cannot prove the loaded lazy pointer non-null, so the
    /// `icmp eq ptr, null` guard survives at EVERY parseValue() use site and fragments GVN's parse-dedup on
    /// repeated-field predicates (q2_original's k-way OR): the diamond strands the last term's parse in the
    /// post-merge block, so fnattr collapses k parses only to 2, not 1. Skipping the guard at TRACE time
    /// (host-level `if`, so the nautilus branch is never emitted) removes the diamond and lets GVN fold the
    /// whole OR chain to ONE parse + a k-wide SIMD compare. MUST NOT be set for JSON input -- the JSON
    /// numeric extractor relies on this {nullptr,0} sentinel to raise FieldNotFound (ERROR 2004) for a used
    /// missing field. Opt-in only; unset = unchanged engine behaviour.
    static const bool assumeValidSpans = std::getenv("NES_ASSUME_VALID_SPANS") != nullptr;
    if (!assumeValidSpans)
    {
        if (fieldAddress == nullptr)
        {
            nautilus::invoke(+[]() { throw FieldNotFound("A projected field is missing from the input (null span at materialisation)."); });
        }
    }
    constexpr InputParserRegistryArguments args{};
    if (const auto parser = InputParserRegistry::instance().create(parserType, args))
    {
        return parser.value()->parseLazyToVarVal(nullable, isNull, fieldAddress, fieldSize);
    }
    throw UnknownInputParserType("Unknown Input Parser: {}", parserType);
}

}
