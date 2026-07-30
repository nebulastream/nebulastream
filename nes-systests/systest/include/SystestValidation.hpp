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

#include <expected>
#include <string_view>

#include <SystestQueryModel.hpp>

namespace NES::Systest
{

class ResultDecoder
{
public:
    virtual ~ResultDecoder() = default;

    virtual std::expected<DecodedTable, ValidationDiagnostic> decode(const TableArtifact&) const = 0;
};

class FileResultDecoder final : public ResultDecoder
{
public:
    std::expected<DecodedTable, ValidationDiagnostic> decode(const TableArtifact&) const override;
};

class ResultComparator
{
public:
    ComparisonResult compare(const RowsExpectation& expected, const DecodedInputStream& actual) const;
    ComparisonResult compare(const DecodedInputStream& expected, const DecodedInputStream& actual) const;
};

class TextComparator
{
public:
    ComparisonResult compare(const TextExpectation& expected, std::string_view actual) const;
};

class CaseValidator
{
public:
    CaseValidator(const ResultDecoder& decoder, const ResultComparator& resultComparator, const TextComparator& textComparator);

    ValidatedResult validate(const ResolvedCase& testCase, const ExecutionOutcome& outcome) const;

private:
    const ResultDecoder& decoder;
    const ResultComparator& resultComparator;
    const TextComparator& textComparator;
};

}
