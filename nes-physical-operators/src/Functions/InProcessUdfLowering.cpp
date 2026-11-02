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


#include <Functions/InProcessUdfLowering.hpp>

#include <utility>
#include <variant>
#include <Functions/PhysicalFunction.hpp>
#include <Functions/UDFPhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{

PhysicalFunction InProcessUdfLowering::createInProcess(UdfLoweringRegistryArguments arguments)
{
    const auto* descriptor = std::get_if<UdfDescriptor>(&arguments.udf);
    INVARIANT(descriptor != nullptr, "An InProcess UDF is always a catalog UDF");
    return PhysicalFunction{
        UDFPhysicalFunction(arguments.childFunctions, descriptor->getArgTypes(), arguments.outputType, UdfBackend::create(*descriptor))}
        .withSetupChildren(std::move(arguments.childFunctions));
}

}
