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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_ANY_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_ANY_HPP_
#include <Nautilus/Util/Casting.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Util/CastUtils.hpp>
#include <memory>
#include <ostream>

namespace NES::Nautilus {

class Any;
typedef std::shared_ptr<Any> AnyPtr;

/**
 * @brief Any is the base class of all data types in the interpreter framework.
 */
class Any : public Typed {
  public:
    Any(const TypeIdentifier* identifier);
    template<typename type, typename... Args>
    static std::shared_ptr<type> create(Args&&... args) {
        return std::make_shared<type>(std::forward<Args>(args)...);
    }

    virtual std::shared_ptr<Any> copy() = 0;
    virtual ~Any() = default;

    template<typename Type>
    const Type& staticCast() const {
        return static_cast<const Type&>(*this);
    }
    virtual std::string toString();

    virtual Nautilus::IR::Types::StampPtr getType() const;
};

class TraceableType : public Any {
  public:
    TraceableType(const TypeIdentifier* identifier) : Any(identifier) {}
    static const TraceableType& asTraceableType(const Any&);
};

template<class X, class Y>
    requires(std::is_same<Any, X>::value)
inline std::shared_ptr<X> cast(const std::shared_ptr<Y>& value) {
    // copy value value
    return value;
}

class Int8;
class Int16;
class Int32;
class Int64;
class UInt8;
class UInt16;
class UInt32;
class UInt64;
class Double;
class Float;
class Boolean;

template<class T>
struct RawTypeToNautilusType;

#define SPECIALIZE_RAW_TO_NAUTILUS_TYPE(RawType, NautilusType)                                                                   \
    template<>                                                                                                                   \
    struct RawTypeToNautilusType<RawType> {                                                                                      \
        typedef NautilusType type;                                                                                               \
    };

SPECIALIZE_RAW_TO_NAUTILUS_TYPE(int8_t, Int8);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(int16_t, Int16);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(int32_t, Int32);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(int64_t, Int64);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(uint8_t, UInt8);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(uint16_t, UInt16);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(uint32_t, UInt32);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(uint64_t, UInt64);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(double, Double);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(float, Float);
SPECIALIZE_RAW_TO_NAUTILUS_TYPE(bool, Boolean);

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_ANY_HPP_
