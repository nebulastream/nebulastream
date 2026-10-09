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

#include <JsonParser.hpp>

#include <simdjson.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

extern "C" void* seq_alloc(size_t size);

namespace
{
enum class JsonKind : int64_t
{
    Null = 0,
    Bool = 1,
    Int = 2,
    Float = 3,
    String = 4,
    Array = 5,
    Object = 6,
};

/// Output of one parse: the flat node list plus the decoded text it points into.
struct FlatJson
{
    std::vector<int64_t> ints;
    std::vector<double> reals;
    std::string text;
    uint64_t nodeCount = 0;

    void addNode(const JsonKind kind, const uint64_t count, const int64_t payload, const double real)
    {
        ints.insert(ints.end(), {static_cast<int64_t>(kind), static_cast<int64_t>(count), payload, 0, 0});
        reals.push_back(real);
        ++nodeCount;
    }

    void addText(const std::string_view value)
    {
        ints.insert(
            ints.end(),
            {static_cast<int64_t>(JsonKind::String), 0, 0, static_cast<int64_t>(text.size()), static_cast<int64_t>(value.size())});
        reals.push_back(0.0);
        text.append(value);
        ++nodeCount;
    }
};

thread_local std::array<int8_t, 4096> parseError;

void setParseError(const std::string_view message, int8_t** errorPointer, uint64_t* errorSize) noexcept
{
    const auto copiedSize = std::min(message.size(), parseError.size());
    std::memcpy(parseError.data(), message.data(), copiedSize);
    *errorPointer = parseError.data();
    *errorSize = copiedSize;
}

void emitElement(FlatJson& out, const simdjson::dom::element& element);

void emitArray(FlatJson& out, const simdjson::dom::element& element)
{
    std::vector<simdjson::dom::element> items;
    for (auto item : element.get_array().value())
    {
        items.push_back(item);
    }
    out.addNode(JsonKind::Array, items.size(), 0, 0.0);
    for (const auto& item : items)
    {
        emitElement(out, item);
    }
}

void emitObject(FlatJson& out, const simdjson::dom::element& element)
{
    std::vector<std::pair<std::string, simdjson::dom::element>> fields;
    for (auto field : element.get_object().value())
    {
        /// simdjson's DOM key is already unescaped; decoding it again would turn an escaped backslash into a control character.
        fields.emplace_back(std::string{field.key}, field.value);
    }
    out.addNode(JsonKind::Object, fields.size(), 0, 0.0);
    for (const auto& [key, value] : fields)
    {
        out.addText(key);
        emitElement(out, value);
    }
}

void emitElement(FlatJson& out, const simdjson::dom::element& element)
{
    switch (element.type())
    {
        case simdjson::dom::element_type::ARRAY:
            emitArray(out, element);
            return;
        case simdjson::dom::element_type::OBJECT:
            emitObject(out, element);
            return;
        case simdjson::dom::element_type::STRING:
            out.addText(element.get_string().value());
            return;
        case simdjson::dom::element_type::BOOL:
            out.addNode(JsonKind::Bool, 0, element.get_bool().value() ? 1 : 0, 0.0);
            return;
        case simdjson::dom::element_type::INT64:
            out.addNode(JsonKind::Int, 0, element.get_int64().value(), 0.0);
            return;
        case simdjson::dom::element_type::UINT64: {
            const auto value = element.get_uint64().value();
            if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
            {
                out.addNode(JsonKind::Int, 0, static_cast<int64_t>(value), 0.0);
            }
            else
            {
                out.addNode(JsonKind::Float, 0, 0, static_cast<double>(value));
            }
            return;
        }
        case simdjson::dom::element_type::DOUBLE:
            out.addNode(JsonKind::Float, 0, 0, element.get_double().value());
            return;
        case simdjson::dom::element_type::NULL_VALUE:
            out.addNode(JsonKind::Null, 0, 0, 0.0);
            return;
    }
}
}

extern "C" uint8_t nes_json_parse(
    const int8_t* inputPointer,
    const uint64_t inputSize,
    int64_t** nodeIntsPointer,
    double** nodeRealsPointer,
    uint64_t* nodeCount,
    int8_t** textPointer,
    uint64_t* textSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    FlatJson out;
    try
    {
        thread_local simdjson::dom::parser parser;
        auto document = parser.parse(reinterpret_cast<const char*>(inputPointer), inputSize);
        emitElement(out, document.value());
    }
    catch (const simdjson::simdjson_error& error)
    {
        setParseError(error.what(), errorPointer, errorSize);
        return 1;
    }
    catch (const std::exception& exception)
    {
        setParseError(exception.what(), errorPointer, errorSize);
        return 1;
    }

    /// seq_alloc keeps results alive for the Codon caller, matching the OpenCV adapter. Zero-sized allocations are avoided.
    auto* ints = static_cast<int64_t*>(seq_alloc(std::max<size_t>(out.ints.size(), 1) * sizeof(int64_t)));
    auto* reals = static_cast<double*>(seq_alloc(std::max<size_t>(out.reals.size(), 1) * sizeof(double)));
    auto* text = static_cast<int8_t*>(seq_alloc(std::max<size_t>(out.text.size(), 1)));
    std::memcpy(ints, out.ints.data(), out.ints.size() * sizeof(int64_t));
    std::memcpy(reals, out.reals.data(), out.reals.size() * sizeof(double));
    std::memcpy(text, out.text.data(), out.text.size());

    *nodeIntsPointer = ints;
    *nodeRealsPointer = reals;
    *nodeCount = out.nodeCount;
    *textPointer = text;
    *textSize = out.text.size();
    return 0;
}
