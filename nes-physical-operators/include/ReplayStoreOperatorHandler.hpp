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
#include <cstdint>
#include <string>

#include <Runtime/Execution/OperatorHandler.hpp>
#include <BinaryStoreWriter.hpp>
#include "Runtime/QueryTerminationType.hpp"

namespace NES
{

/// Operator handler that delagates file I/O to ReplayStoreWriter
class ReplayStoreOperatorHandler final : public OperatorHandler
{
public:
    struct Config
    {
        std::string filePath;
        std::string schemaText;
    };

    explicit ReplayStoreOperatorHandler(Config cfg);
    ~ReplayStoreOperatorHandler() override = default;

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void ensureHeader(PipelineExecutionContext& pec);

    void append(const uint8_t* data, size_t len);

private:
    StoreManager::BinaryStoreWriter writer;
};

}
