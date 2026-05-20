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
#include <memory>
#include <string>
#include <vector>
#include <Model.hpp>
#include <RuntimeBackend.hpp>

#include <iree/runtime/api.h>

namespace NES
{
struct InstanceDeleter
{
    void operator()(iree_runtime_instance_t* ptr) const
    {
        if (ptr != nullptr)
        {
            iree_runtime_instance_release(ptr);
        }
    }
};

struct SessionDeleter
{
    void operator()(iree_runtime_session_t* ptr) const
    {
        if (ptr != nullptr)
        {
            iree_runtime_session_release(ptr);
        }
    }
};

class IreeRuntimeBackend final : public RuntimeBackend
{
public:
    RuntimeMetadata setup(const CompiledModel& model) override;
    void infer(std::byte* inputBuffer, size_t, std::byte* outputBuffer, size_t outputBufferSize) override;

private:
    std::unique_ptr<iree_runtime_instance_t, InstanceDeleter> instance;
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> session;
    iree_vm_function_t function{};
    std::vector<size_t> inputShape;
    size_t nDim = 0;
    std::string functionName;
};
}
