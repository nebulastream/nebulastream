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

#include "IREEInferenceOperator.hpp"
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Common.hpp>
#include <nautilus/function.hpp>

#include "IREEInferenceOperatorHandler.hpp"

namespace NES::Runtime::Execution::Operators
{

template<class T>
void addValueToModel(int index, float value, void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    adapter->addModelInput(index, value);
}

void applyModel(void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    return adapter->getResultAt(index);
}

void IREEInferenceOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const
{
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    for (int i = 0; i < inputFieldNames.size(); i++)
    {
        VarVal value = record.read(inputFieldNames.at(nautilus::static_val<int>(i)));
        nautilus::invoke(addValueToModel<float>, nautilus::val<int>(i), value.cast<nautilus::val<float>>(), inferModelHandler);
    }

    nautilus::invoke(applyModel, inferModelHandler);

    for (int i = 0; i < outputFieldNames.size(); i++)
    {
        VarVal result = VarVal(nautilus::invoke(getValueFromModel, nautilus::val<int>(i), inferModelHandler));
        record.write(outputFieldNames.at(i), result);
    }

    child->execute(ctx, record);
}

}