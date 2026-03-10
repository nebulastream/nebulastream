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

#include <Model.hpp>

namespace NES::Inference
{

bool Model::operator==(const Model& rhs) const
{
    if (byteCode != rhs.byteCode || shape != rhs.shape || outputShape != rhs.outputShape || functionName != rhs.functionName
        || dims != rhs.dims || outputDims != rhs.outputDims || inputSizeInBytes != rhs.inputSizeInBytes
        || outputSizeInBytes != rhs.outputSizeInBytes)
    {
        return false;
    }
    if (inputs.size() != rhs.inputs.size() || outputs.size() != rhs.outputs.size())
    {
        return false;
    }
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (*inputs[i] != *rhs.inputs[i])
        {
            return false;
        }
    }
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (outputs[i].first != rhs.outputs[i].first || *outputs[i].second != *rhs.outputs[i].second)
        {
            return false;
        }
    }
    return true;
}

}
