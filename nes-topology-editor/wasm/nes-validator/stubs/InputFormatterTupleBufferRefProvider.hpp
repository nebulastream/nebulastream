#pragma once
// Stub for WASM build -- SourceCatalog.cpp includes this but validation doesn't use formatters
#include <memory>
#include <string>
#include <unordered_map>

namespace NES {
struct ParserConfig;  // forward declare -- real definition is in SourceDescriptor.hpp
class InputFormatterTupleBufferRef;
class TupleBufferRef;

inline std::shared_ptr<InputFormatterTupleBufferRef>
provideInputFormatterTupleBufferRef(ParserConfig, std::shared_ptr<TupleBufferRef>) { return nullptr; }

// Always return true -- WASM validation doesn't need to check parser type registration
inline bool contains(const std::string&) { return true; }
}
