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

#include <Functions/PhysicalFunction.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{

/// Lowers inline Python UDFs with BRIDGE 'cpython' or 'pypy', which run in the interpreter embedded by that bridge's library.
/// Catalog UDFs with these bridges need no lowering of their own: their descriptor names the bridge library, so they are
/// InProcess UDFs.
struct PythonInterpreterUdfLowering
{
    static PhysicalFunction createCPython(UdfLoweringRegistryArguments arguments);
    static PhysicalFunction createPyPy(UdfLoweringRegistryArguments arguments);
};

}
