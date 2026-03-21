#pragma once
// Stub for WASM build -- SourceCatalog.cpp includes this but validation doesn't use formatters.
// Parser type validation IS provided: we check against the known registered types from
// InputFormatIndexerGeneratedRegistrar.inc (CSV, JSON) to match nes-cli behavior.
#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>

namespace NES {
struct ParserConfig;  // forward declare -- real definition is in SourceDescriptor.hpp
class InputFormatterTupleBufferRef;
class TupleBufferRef;

inline std::shared_ptr<InputFormatterTupleBufferRef>
provideInputFormatterTupleBufferRef(ParserConfig, std::shared_ptr<TupleBufferRef>) { return nullptr; }

/// Check if a parser type is registered. Uses the same type names as
/// InputFormatIndexerGeneratedRegistrar.inc (case-insensitive, matching Registry behavior).
inline bool contains(const std::string& parserType) {
    // Must match the entries in generated/InputFormatIndexerGeneratedRegistrar.inc
    static constexpr std::array<std::string_view, 2> REGISTERED_PARSER_TYPES = {"CSV", "JSON"};
    // Registry is case-insensitive (toUpperCase on lookup), so compare uppercase
    std::string upper = parserType;
    for (auto& c : upper) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return std::ranges::find(REGISTERED_PARSER_TYPES, upper) != REGISTERED_PARSER_TYPES.end();
}
}
