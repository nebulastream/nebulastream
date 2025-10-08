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

#include <IREEInferenceOperatorHandler.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>

namespace NES
{

struct IREENautilusProxies
{
IREENautilusProxies(bool isBatch) : isBatch(isBatch) {}

template <class T>
void addValueToModel(int index, float value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = isBatch
        ? static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler)
        : static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(index, value);
}

void copyVarSizedFromModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = isBatch
        ? static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler)
        : static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->copyResultTo(std::span{content, size});
}

void copyVarSizedToModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = isBatch
        ? static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler)
        : static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void applyModel(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = isBatch
        ? static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler)
        : static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = isBatch
        ? static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler)
        : static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    return adapter->getResultAt(index);
}

template <typename T>
nautilus::val<T> min(const nautilus::val<T>& lhs, const nautilus::val<T>& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

void garbageCollectBatchesProxy(void* inferModelHandler)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    handler->garbageCollectBatches();
}

private:
    bool isBatch;
};

}
