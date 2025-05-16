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

#include <iostream>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
/// Following functions are needed to support these serialized types in the configuration descriptor
inline std::ostream& operator<<(std::ostream& os, const FunctionList& list)
{
    os << list.DebugString();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const AggregationFunctionList& list)
{
    os << list.DebugString();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const WindowInfos& infos)
{
    os << infos.DebugString();
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const SerializableFunction& func)
{
    os << func.DebugString();
    return os;
}

inline bool operator==(const FunctionList& lhs, const FunctionList& rhs)
{
    /// Compare by serializing to string.
    return lhs.SerializeAsString() == rhs.SerializeAsString();
}

inline bool operator==(const AggregationFunctionList& lhs, const AggregationFunctionList& rhs)
{
    /// Compare by serializing to string.
    return lhs.SerializeAsString() == rhs.SerializeAsString();
}

inline bool operator==(const WindowInfos& lhs, const WindowInfos& rhs)
{
    /// Compare by serializing to string.
    return lhs.SerializeAsString() == rhs.SerializeAsString();
}

inline bool operator==(const SerializableFunction& lhs, const SerializableFunction& rhs)
{
    /// Compare by serializing to string.
    return lhs.SerializeAsString() == rhs.SerializeAsString();
}

}
