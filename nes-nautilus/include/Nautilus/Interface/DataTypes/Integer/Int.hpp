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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INTEGER_INT_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INTEGER_INT_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
namespace NES::Nautilus {

/**
 * @brief Abstract integer data type.
 */
class Int : public TraceableType {
  public:
    explicit Int(const TypeIdentifier* identifier);
    virtual ~Int();
    virtual const std::shared_ptr<Int> add(const Int&) const = 0;
    virtual const std::shared_ptr<Int> sub(const Int&) const = 0;
    virtual const std::shared_ptr<Int> div(const Int&) const = 0;
    virtual const std::shared_ptr<Int> mod(const Int&) const = 0;
    virtual const std::shared_ptr<Int> mul(const Int&) const = 0;
    virtual const std::shared_ptr<Int> bitWiseAnd(const Int&) const = 0;
    virtual const std::shared_ptr<Int> bitWiseOr(const Int&) const = 0;
    virtual const std::shared_ptr<Int> bitWiseXor(const Int&) const = 0;
    virtual const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const = 0;
    virtual const std::shared_ptr<Int> bitWiseRightShift(const Int&) const = 0;
    virtual const std::shared_ptr<Boolean> equals(const Int&) const = 0;
    virtual const std::shared_ptr<Boolean> lessThan(const Int& other) const = 0;
    virtual const std::shared_ptr<Boolean> greaterThan(const Int& other) const = 0;
    virtual int64_t getRawInt() const = 0;
    static bool isInteger(const Any&);
};

/**
 * @brief Int8 data type.
 */
class Int8 : public Int {
  public:
    using RawType = int8_t;
    static const inline auto type = TypeIdentifier::create<Int8>();
    Int8(int8_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& other) const override;
    const std::shared_ptr<Int> div(const Int& other) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& other) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& other) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    int8_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    int8_t value;
};

/**
 * @brief Int16 data type.
 */
class Int16 : public Int {
  public:
    using RawType = int16_t;
    static const inline auto type = TypeIdentifier::create<Int16>();
    Int16(int16_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    int16_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    int16_t value;
};

/**
 * @brief Int32 data type.
 */
class Int32 : public Int {
  public:
    using RawType = int32_t;
    static const inline auto type = TypeIdentifier::create<Int32>();
    Int32(int32_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    int32_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    int32_t value;
};

/**
 * @brief Int64 data type.
 */
class Int64 : public Int {
  public:
    using RawType = int64_t;
    static const inline auto type = TypeIdentifier::create<Int64>();
    Int64(int64_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    int64_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    int64_t value;
};

/**
 * @brief UInt8 data type.
 */
class UInt8 : public Int {
  public:
    using RawType = uint8_t;
    static const inline auto type = TypeIdentifier::create<UInt8>();
    UInt8(uint8_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    uint8_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    uint8_t value;
};

/**
 * @brief UInt16 data type.
 */
class UInt16 : public Int {
  public:
    using RawType = uint16_t;
    static const inline auto type = TypeIdentifier::create<UInt16>();
    UInt16(uint16_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    uint16_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    uint16_t value;
};

/**
 * @brief UInt32 data type.
 */
class UInt32 : public Int {
  public:
    using RawType = uint32_t;
    static const inline auto type = TypeIdentifier::create<UInt32>();
    UInt32(uint32_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    uint32_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    uint32_t value;
};

/**
 * @brief UInt64 data type.
 */
class UInt64 : public Int {
  public:
    using RawType = uint64_t;
    static const inline auto type = TypeIdentifier::create<UInt64>();
    UInt64(uint64_t value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    const std::shared_ptr<Int> add(const Int& other) const override;
    const std::shared_ptr<Int> sub(const Int& anInt) const override;
    const std::shared_ptr<Int> div(const Int& anInt) const override;
    const std::shared_ptr<Int> mod(const Int& anInt) const override;
    const std::shared_ptr<Int> mul(const Int& anInt) const override;
    const std::shared_ptr<Int> bitWiseAnd(const Int&) const override;
    const std::shared_ptr<Int> bitWiseOr(const Int&) const override;
    const std::shared_ptr<Int> bitWiseXor(const Int&) const override;
    const std::shared_ptr<Int> bitWiseLeftShift(const Int&) const override;
    const std::shared_ptr<Int> bitWiseRightShift(const Int&) const override;
    const std::shared_ptr<Boolean> equals(const Int& anInt) const override;
    const std::shared_ptr<Boolean> lessThan(const Int& other) const override;
    const std::shared_ptr<Boolean> greaterThan(const Int& other) const override;
    uint64_t getValue() const;
    int64_t getRawInt() const override;
    std::string toString() override;

  private:
    uint64_t value;
};

}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INTEGER_INT_HPP_
