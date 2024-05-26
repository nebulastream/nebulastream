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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/ExternalAPI.hpp>
#include <Compiler/SourceCode.hpp>
#include <DataParser/ParserFactory.hpp>
#include <Runtime/FormatType.hpp>
#include <algorithm>
#include <fmt/format.h>
#include <sstream>
#include <string_view>

namespace NES::DataParser {

class ParserImpl : public Parser {
  public:
    [[nodiscard]] std::string_view parseIntoBuffer(std::string_view data, NES::Runtime::TupleBuffer& tb) const override {
        return fn(data, tb);
    }

    [[nodiscard]] std::string_view parse(std::string_view data) const override { return parseFn(data); }

    [[nodiscard]] std::vector<void*> getFieldOffsets() const override { return fieldOffsetFn(); }

    ParserImpl(const std::function<std::string_view(std::string_view, NES::Runtime::TupleBuffer&)>& fn,
               const std::function<std::string_view(std::string_view)>& parseFn,
               const std::function<std::vector<void*>()>& fieldOffsetFn,
               std::shared_ptr<Compiler::DynamicObject> shared_library)
        : fn(fn), parseFn(parseFn), fieldOffsetFn(fieldOffsetFn), sharedLibrary(std::move(shared_library)) {}

  private:
    std::function<std::string_view(std::string_view, NES::Runtime::TupleBuffer&)> fn;
    std::function<std::string_view(std::string_view)> parseFn;
    std::function<std::vector<void*>()> fieldOffsetFn;
    std::shared_ptr<Compiler::DynamicObject> sharedLibrary;
};

static std::string_view typeMapping(DataTypePtr dt) {
    using namespace std::literals;
    auto df = DefaultPhysicalTypeFactory();
    auto physicalType = df.getPhysicalType(dt);
    if (physicalType->isBasicType()) {
        auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicPhysicalType->nativeType) {
            case NES::BasicPhysicalType::NativeType::INT_8: return "INT8"sv;
            case NES::BasicPhysicalType::NativeType::INT_16: return "INT16"sv;
            case NES::BasicPhysicalType::NativeType::INT_32: return "INT32"sv;
            case NES::BasicPhysicalType::NativeType::INT_64: return "INT64"sv;
            case NES::BasicPhysicalType::NativeType::UINT_8: return "UINT8"sv;
            case NES::BasicPhysicalType::NativeType::UINT_16: return "UINT16"sv;
            case NES::BasicPhysicalType::NativeType::UINT_32: return "UINT32"sv;
            case NES::BasicPhysicalType::NativeType::UINT_64: return "UINT64"sv;
            case NES::BasicPhysicalType::NativeType::FLOAT: return "FLOAT32"sv;
            case NES::BasicPhysicalType::NativeType::DOUBLE: return "FLOAT64"sv;
            case NES::BasicPhysicalType::NativeType::TEXT: return "TEXT"sv;
            case NES::BasicPhysicalType::NativeType::BOOLEAN: return "BOOLEAN"sv;
            default: NES_NOT_IMPLEMENTED();
        }
    }

    NES_NOT_IMPLEMENTED();
}

class ScnLibExternalAPI : public Compiler::ExternalAPI {
  public:
    const Compiler::CompilerFlags getCompilerFlags() const override {
        Compiler::CompilerFlags flags;
        flags.addFlag("-O3");
        flags.addFlag("-march=native");
        flags.addFlag("-fvisibility=hidden");
        if (format == FormatTypes::CSV_FORMAT) {
            flags.addFlag("-lscn");
            flags.addFlag("-lsimdutf");
        } else if (format == FormatTypes::JSON_FORMAT) {
            flags.addFlag(PATH_TO_NES_SOURCE_CODE "/nes-data-parser/include/generation/simdjson.cpp");
        }
        flags.addFlag("-Wl,--exclude-libs,ALL");
        return flags;
    }

  private:
    NES::FormatTypes format;

  public:
    explicit ScnLibExternalAPI(NES::FormatTypes format) : format(format) {}
};

static std::string_view getAdapterName(NES::FormatTypes format) {
    using namespace std::literals;
    switch (format) {
        case NES::FormatTypes::JSON_FORMAT: return "JSON::JSONAdapter"sv;
        case NES::FormatTypes::CSV_FORMAT: return "CSV::CSVAdapter"sv;
        default: NES_NOT_IMPLEMENTED();
    }
}

static std::string_view getAdapterInclude(FormatTypes format) {
    using namespace std::literals;
    switch (format) {
        case NES::FormatTypes::CSV_FORMAT: return "#include <generation/CSVAdapter.hpp>"sv;
        case NES::FormatTypes::JSON_FORMAT: return "#include <generation/JSONAdapter.hpp>"sv;
        default: NES_NOT_IMPLEMENTED();
    }
}

template<typename F, typename W, typename R>
struct helper {
    F f;
    W w;

    helper(F f, W w) : f(std::move(f)), w(std::move(w)) {}

    helper(const helper& other) : f(other.f), w(other.w) {}

    helper(helper&& other) : f(std::move(other.f)), w(std::move(other.w)) {}

    helper& operator=(helper other) {
        f = std::move(other.f);
        w = std::move(other.w);
        return *this;
    }

    R operator()() {
        f.wait();
        return w(std::move(f));
    }
};

template<typename Fut, typename W>
auto then(Fut f, W w) {
    return std::async(std::launch::async, helper<Fut, W, decltype(w(std::move(f)))>(std::move(f), std::move(w)));
}

std::future<std::unique_ptr<NES::DataParser::Parser>>
NES::DataParser::ParserFactory::createParser(const NES::Schema& schema, NES::FormatTypes format) const {

    size_t bufferSize = 8192;

    std::vector<std::string> fields;
    std::transform(schema.fields.begin(), schema.fields.end(), std::back_inserter(fields), [](const auto& field) {
        return fmt::format("NES::DataParser::Field<NES::DataParser::{},\"{}\">",
                           typeMapping(field->getDataType()),
                           field->getName());
    });

    std::stringstream ss;

    ss << "#include <Runtime/TupleBuffer.hpp>\n";
    ss << "#include <generation/SchemaBuffer.hpp>\n";
    ss << getAdapterInclude(format) << '\n';

    ss << fmt::format("using Schema = NES::DataParser::StaticSchema<{}>;\n", fmt::join(fields, ","));
    ss << fmt::format("using SchemaBuffer = NES::DataParser::SchemaBuffer<Schema, {}>;\n", bufferSize);
    ss << fmt::format("using Adapter = NES::DataParser::{}<SchemaBuffer>;\n", getAdapterName(format));

    ss << R"(extern "C" __attribute__((visibility("default"))) std::string_view impl(std::string_view view, NES::Runtime::TupleBuffer& tb) {
              return Adapter::parseIntoBuffer(view, tb);
          })"
       << '\n';

    ss << "static thread_local Schema::TupleType tuple;\n";
    ss << R"(extern "C" __attribute__((visibility("default"))) std::string_view parse(std::string_view view) {
              std::string_view left;
              std::tie(left, tuple) = Adapter::parseAndAdvance(view);
              return left;
          })"
       << '\n';

    ss << R"(extern "C" __attribute__((visibility("default"))) std::vector<void*> fieldOffsets() {
              std::vector<void*> results;
              for(auto offset: Schema::Offsets) {
                  results.push_back(static_cast<void*>(reinterpret_cast<std::byte*>(std::addressof(tuple)) + offset));
              }
              return results;
          })"
       << '\n';

    auto sourceCode = ss.str();

    char name[L_tmpnam];
    tmpnam(name);

    auto request = std::make_unique<Compiler::CompilationRequest>(
        std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, sourceCode),
        name,
        false,
        false,
        false,
        true,
        std::vector<std::shared_ptr<Compiler::ExternalAPI>>{std::make_shared<ScnLibExternalAPI>(format)});

    auto result = jitCompiler->compile(std::move(request));
    return then(std::move(result), [](std::future<Compiler::CompilationResult> result) -> std::unique_ptr<Parser> {
        auto resolved = result.get();
        auto impl =
            resolved.getDynamicObject()->getInvocableMember<std::string_view (*)(std::string_view, NES::Runtime::TupleBuffer&)>(
                "impl");
        auto offsets = resolved.getDynamicObject()->getInvocableMember<std::vector<void*> (*)()>("fieldOffsets");
        auto parse = resolved.getDynamicObject()->getInvocableMember<std::string_view (*)(std::string_view)>("parse");

        return std::make_unique<ParserImpl>(impl, parse, offsets, resolved.getDynamicObject());
    });
}
}// namespace NES::DataParser
