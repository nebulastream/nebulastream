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
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

enum class QuotationType : uint8_t
{
    NONE,
    DOUBLE_QUOTE
};

void parseRawValueIntoRecord(
    DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    QuotationType quotationType,
    const std::string& parserType,
    const bool& lazyOverload,
    Characteristic varSizedCharacteristics);

/// Host-time: the byte-property characteristics an input formatter asserts for its VARSIZED/CHAR passthrough
/// values (see Characteristic). A formatter's metadata may state it explicitly by exposing
/// `getVarSizedCharacteristics()` (the CSV family does -- driven by its `assume_clean_strings` config); otherwise
/// this derives a safe default from the quotation style: a JSON string body (DOUBLE_QUOTE) is a valid JSON string
/// body (JSON_ESCAPED), while any other raw field (NONE) asserts nothing and keeps the escaping path. The result
/// is a host constant, so the output formatter's passthrough gate folds it at trace (zero per-row cost).
template <typename IndexerMetaData>
[[nodiscard]] Characteristic varSizedCharacteristicsOf(const IndexerMetaData& metaData)
{
    if constexpr (requires { metaData.getVarSizedCharacteristics(); })
    {
        return metaData.getVarSizedCharacteristics();
    }
    else
    {
        return metaData.getQuotationType() == QuotationType::DOUBLE_QUOTE ? Characteristic::JSON_ESCAPED : Characteristic::NONE;
    }
}

/// We expect a pointer and the size so that we can use this method from the nautilus runtime
bool checkIsNullProxy(const int8_t* fieldAddress, uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept;


/// Instantiate the input parser for this value and parse the value with it
/// Todo: This might not be needed anymore on this branch
VarVal parseFixedSizeIntoVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::string& parserType);

VarVal parseLazyIntoVarVal(
    bool nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& parserType);
}
