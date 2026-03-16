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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <ReplayStoreOperatorHandler.hpp>
#include "Runtime/Execution/OperatorHandler.hpp"

#include <Runtime/QueryTerminationType.hpp>

namespace NES
{

ReplayStoreOperatorHandler::ReplayStoreOperatorHandler(Config cfg)
    : writer(StoreManager::BinaryStoreWriter::Config{
          .storeName = std::move(cfg.storeName),
          .filePath = std::move(cfg.filePath),
          .schemaText = std::move(cfg.schemaText),
      })
{
}

void ReplayStoreOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    writer.open();
    writer.ensureHeader();
}

void ReplayStoreOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    writer.close();
}

void ReplayStoreOperatorHandler::ensureHeader(PipelineExecutionContext&)
{
    writer.ensureHeader();
}

void ReplayStoreOperatorHandler::append(const uint8_t* data, size_t len)
{
    writer.append(data, len);
}

int8_t* ReplayStoreOperatorHandler::getRowBuffer(uint32_t rowWidth)
{
    if (rowBuffer.size() < rowWidth)
    {
        rowBuffer.resize(rowWidth);
    }
    return reinterpret_cast<int8_t*>(rowBuffer.data());
}

void ReplayStoreOperatorHandler::commitRow(uint32_t len)
{
    writer.append(rowBuffer.data(), static_cast<size_t>(len));
}

}
