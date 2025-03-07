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

#include <memory>
#include <string>
#include <API/Schema.hpp>
#include <InputFormatters/AsyncInputFormatterTask.hpp>
#include <InputFormatters/SyncInputFormatterTask.hpp>
#include <Util/Notifier.hpp>

namespace NES::InputFormatters::InputFormatterProvider
{
std::unique_ptr<AsyncInputFormatterTask> provideAsyncInputFormatterTask(
    OriginId originId,
    const std::string& parserType,
    std::shared_ptr<Schema> schema,
    std::string tupleDelimiter,
    std::string fieldDelimiter);

std::unique_ptr<SyncInputFormatterTask> provideSyncInputFormatterTask(
    OriginId originId,
    const std::string& parserType,
    std::shared_ptr<Schema> schema,
    std::string tupleDelimiter,
    std::string fieldDelimiter,
    std::optional<std::shared_ptr<Notifier>> syncInputFormatterTaskNotifier);
}
