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

#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// SQL functions that our engine computes with its other functions, e.g. `a <> b` as `NOT (a = b)`. The registry keys are the
/// functions' Substrait names. Each function returns its output type.
struct SqlPhysicalFunction
{
    static PhysicalFunctionRegistryReturnType createNot_Equal(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIs_Not_Null(PhysicalFunctionRegistryArguments arguments);
    /// NULL or false.
    static PhysicalFunctionRegistryReturnType createIs_Not_True(PhysicalFunctionRegistryArguments arguments);
    /// NaN is the only value that is not equal to itself. Other types than floating point are never NaN.
    static PhysicalFunctionRegistryReturnType createIs_Nan(PhysicalFunctionRegistryArguments arguments);
    /// e to the power of the argument.
    static PhysicalFunctionRegistryReturnType createExp(PhysicalFunctionRegistryArguments arguments);
    /// 0 - x.
    static PhysicalFunctionRegistryReturnType createNegate(PhysicalFunctionRegistryArguments arguments);
    /// Text to a number parses the text. Text to text keeps it (only the length of the type differs).
    /// A number, boolean or character to text formats it.
    static PhysicalFunctionRegistryReturnType createCast(PhysicalFunctionRegistryArguments arguments);

    /// A timestamp is unix milliseconds (UINT64).
    /// TIMESTAMP_MILLIS(milliseconds) and UNIX_MILLIS(timestamp) only change the type.
    static PhysicalFunctionRegistryReturnType createTimestamp_Millis(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createUnix_Millis(PhysicalFunctionRegistryArguments arguments);
    /// EXTRACT(DAY|MONTH|YEAR FROM timestamp) in UTC: (unit, timestamp) or (unit, indexing, timestamp) with indexing ONE.
    static PhysicalFunctionRegistryReturnType createExtract(PhysicalFunctionRegistryArguments arguments);
    /// FORMAT_DATETIME(format, timestamp) for the format of ISO 8601 in UTC with milliseconds (%Y-%m-%dT%H:%M:%E3SZ).
    static PhysicalFunctionRegistryReturnType createFormat_Datetime(PhysicalFunctionRegistryArguments arguments);
    /// TIMESTAMP(text) of ISO 8601 text.
    static PhysicalFunctionRegistryReturnType createTimestamp(PhysicalFunctionRegistryArguments arguments);
};

}
