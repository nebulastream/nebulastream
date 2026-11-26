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
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <AsyncOperatorUnreflectionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
struct AsyncTestOperatorConfig
{
    std::string label;
    std::vector<DataType> resultTypes;
    int marker = 0;
    std::vector<AsyncInputAccessor> accessors;

    [[nodiscard]] std::string_view getType() const { return label; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const
    {
        std::vector<UnqualifiedUnboundField> fields;
        for (size_t i = 0; i < resultTypes.size(); ++i)
        {
            fields.emplace_back(Identifier::parse("OUT" + std::to_string(i)), resultTypes[i]);
        }
        return Schema<UnqualifiedUnboundField, Ordered>{std::move(fields)};
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, const AsyncRowHandle&) const
    {
        return std::unexpected(AsyncError{.exception = IOError("Unsupported test operator")});
    }
};

inline std::vector<RuntimeValue> readAsyncTestAccessors(const AsyncCall::Ptr& call, const AsyncRowHandle& row)
{
    const auto config = call->getAsyncOperator().getAs<AsyncTestOperatorConfig>();
    std::vector<RuntimeValue> values;
    for (const auto& accessor : config->accessors)
    {
        values.push_back(accessor(row));
    }
    return values;
}

inline AsyncOperator
makeAsyncTestOperator(std::string label, std::vector<DataType> resultTypes, int marker, std::vector<AsyncInputAccessor> accessors)
{
    static_cast<void>(
        AsyncOperatorUnreflectionRegistry::instance().addEntry(label,
                                                               [](const Reflected& data, const ReflectionContext& context) -> AsyncOperator
                                                               { return context.unreflect<AsyncTestOperatorConfig>(data); }));
    return AsyncTestOperatorConfig{
        .label = std::move(label), .resultTypes = std::move(resultTypes), .marker = marker, .accessors = std::move(accessors)};
}

inline AsyncOperator makeAsyncTestOperator(std::string label, std::vector<DataType> resultTypes, int marker)
{
    return makeAsyncTestOperator(std::move(label), std::move(resultTypes), marker, {});
}

inline AsyncOperator makeAsyncTestOperator(std::string label, std::vector<DataType> resultTypes)
{
    return makeAsyncTestOperator(std::move(label), std::move(resultTypes), 0, {});
}
}
