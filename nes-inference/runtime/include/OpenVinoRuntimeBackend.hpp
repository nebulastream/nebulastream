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
#include <openvino/core/shape.hpp>
#include <openvino/core/type/element_type.hpp>
#include <openvino/runtime/infer_request.hpp>
#include <Model.hpp>
#include <RuntimeBackend.hpp>

namespace NES
{
class OpenVinoRuntimeBackend final : public RuntimeBackend
{
public:
    RuntimeMetadata setup(const CompiledModel& model) override;
    void infer(std::byte* inputBuffer, size_t, std::byte* outputBuffer, size_t outputBufferSize) override;

private:
    ov::InferRequest inferRequest;
    ov::element::Type inputElementType;
    ov::Shape inputShape;
    ov::element::Type outputElementType;
    ov::Shape outputShape;
};
}
