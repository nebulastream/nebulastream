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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <InputFormatters/AsyncInputFormatterTask.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Util/Notifier.hpp>
#include <AsyncInputFormatterRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SyncInputFormatterRegistry.hpp>

namespace NES::InputFormatters::InputFormatterProvider
{

std::unique_ptr<AsyncInputFormatterTask> provideAsyncInputFormatterTask(
    const OriginId originId,
    const std::string& inputFormatterType,
    std::shared_ptr<Schema> schema,
    std::string tupleDelimiter,
    std::string fieldDelimiter)
{
    if (auto inputFormatter = AsyncInputFormatterRegistry::instance().create(inputFormatterType, AsyncInputFormatterRegistryArguments{}))
    {
        return std::make_unique<AsyncInputFormatterTask>(
            originId, std::move(tupleDelimiter), std::move(fieldDelimiter), std::move(schema), std::move(inputFormatter.value()));
    }
    throw UnknownInputFormatterType("unknown type of parser: {}", inputFormatterType);
}
std::unique_ptr<SyncInputFormatterTask> provideSyncInputFormatterTask(
    OriginId originId,
    const std::string& inputFormatterType,
    std::shared_ptr<Schema> schema,
    std::string tupleDelimiter,
    std::string fieldDelimiter,
    std::optional<std::shared_ptr<Notifier>> syncInputFormatterTaskNotifier)
{
    PRECONDITION(syncInputFormatterTaskNotifier.has_value(), "A SyncInputFormatterTask must have an Notifier.");
    if (auto inputFormatter = SyncInputFormatterRegistry::instance().create(inputFormatterType, SyncInputFormatterRegistryArguments{}))
    {
        return std::make_unique<SyncInputFormatterTask>(
            originId,
            std::move(tupleDelimiter),
            std::move(fieldDelimiter),
            *schema,
            std::move(inputFormatter.value()),
            std::move(syncInputFormatterTaskNotifier.value()));
    }
    throw UnknownInputFormatterType("unknown type of parser: {}", inputFormatterType);
}
}
