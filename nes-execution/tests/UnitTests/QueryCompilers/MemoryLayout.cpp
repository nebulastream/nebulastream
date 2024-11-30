//
// Created by ls on 11/30/24.
//

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <variant>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <std/cstring.h>

#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

template <typename T>
constexpr std::string_view typeName()
{
    std::string_view sv(__PRETTY_FUNCTION__);
    sv.remove_prefix(std::string_view("std::string_view typeName() [T = ").size());
    sv.remove_suffix(1);
    return sv;
}

namespace NES::Nautilus
{
#define DEFINE_OPERATOR_VAR_VAL_BINARY(operatorName, op) \
    ScalarVarVal operatorName(const ScalarVarVal& rhs) const \
    { \
        return std::visit( \
            [&]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) \
            { \
                if constexpr (requires(LHS l, RHS r) { l op r; }) \
                { \
                    return detail::var_val_t(lhsVal op rhsVal); \
                } \
                else \
                { \
                    throw UnsupportedOperation( \
                        std::string("ScalarVarVal operation not implemented: ") + " " + #operatorName + " " + typeid(LHS).name() + " " \
                        + typeid(RHS).name()); \
                    return detail::var_val_t(lhsVal); \
                } \
            }, \
            this->value, \
            rhs.value); \
    }

#define DEFINE_OPERATOR_VAR_VAL_UNARY(operatorName, op) \
    ScalarVarVal operatorName() const \
    { \
        return std::visit( \
            [&]<typename RHS>(const RHS& rhsVal) \
            { \
                if constexpr (!requires(RHS r) { op r; }) \
                { \
                    throw UnsupportedOperation( \
                        std::string("ScalarVarVal operation not implemented: ") + " " + #operatorName + " " \
                        + typeid(decltype(rhsVal)).name()); \
                    return detail::var_val_t(rhsVal); \
                } \
                else \
                { \
                    detail::var_val_t result = op rhsVal; \
                    return result; \
                } \
            }, \
            this->value); \
    }

#define EVALUATE_FUNCTION(func) \
    [&](const auto& val) \
    { \
        if constexpr (!requires { func(val); }) \
        { \
            throw UnsupportedOperation(std::string("ScalarVarVal function not implemented: ") + typeid(decltype(val)).name()); \
            return NES::Nautilus::detail::var_val_t(val); \
        } \
        else \
        { \
            NES::Nautilus::detail::var_val_t result = func(val); \
            return result; \
        } \
    }


namespace detail
{
template <typename... T>
using var_val_helper = std::variant<nautilus::val<T>...>;
using var_val_t = var_val_helper<bool, char, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;
using var_val_ptr_t
    = var_val_helper<bool*, char*, uint8_t*, uint16_t*, uint32_t*, uint64_t*, int8_t*, int16_t*, int32_t*, int64_t*, float*, double*>;


/// Lookup if a T is in a Variant
template <class T, class U>
struct is_one_of;

template <class T, class... Ts>
struct is_one_of<T, std::variant<Ts...>> : std::bool_constant<(std::is_same_v<T, Ts> || ...)>
{
};
}


/// This class is the base class for all data types in our Nautilus-Backend.
/// It sits on top of the nautilus library and its val<> data type.
/// We derive all specific data types, e.g., variable and fixed data types, from this base class.
/// This class provides all functionality so that a developer does not have to know of any nautilus::val<> and how to work with them.
class ScalarVarVal
{
public:
    /// Construct a ScalarVarVal object for example via ScalarVarVal(32)
    template <typename T>
    explicit ScalarVarVal(const T t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(nautilus::val<T>(t))
    {
    }

    /// Construct via ScalarVarVal(nautilus::val<int>(32)), also allows conversion from static to dynamic
    template <typename T>
    ScalarVarVal(const nautilus::val<T> t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(t)
    {
    }

    /// Construct a ScalarVarVal object for all other types that are part of var_val_helper but are not wrapped
    /// in a nautilus::val<> can be constructed via this constructor, e.g, ScalarVarVal(VariableSize).
    template <typename T>
    ScalarVarVal(const T t)
    requires(detail::is_one_of<T, detail::var_val_t>::value)
        : value(t)
    {
    }

    ScalarVarVal(const ScalarVarVal& other);
    ScalarVarVal(ScalarVarVal&& other) noexcept;
    ScalarVarVal& operator=(const ScalarVarVal& other);
    ScalarVarVal& operator=(ScalarVarVal&& other);
    explicit operator bool() const;
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const ScalarVarVal& varVal);

    /// Casts the underlying value to the given type T1. If the cast is not possible, a std::bad_variant_access exception is thrown.
    /// This should be the only way how the underlying value can be accessed.
    template <typename T1>
    T1 cast() const
    {
        if (std::holds_alternative<T1>(value))
        {
            return std::get<T1>(value);
        }
        auto strType = std::visit([](auto&& x) -> decltype(auto) { return typeName<decltype(x)>(); }, value);
        throw UnsupportedOperation("Tried to cast to {} but was type {}", typeName<T1>(), strType);
    }

    template <typename T>
    ScalarVarVal customVisit(T t) const
    {
        return std::visit(t, value);
    }

    /// Defining operations on ScalarVarVal. In the macro, we use std::variant and std::visit to automatically call the already
    /// existing operations on the underlying nautilus::val<> data types.
    /// For the VarSizedDataType, we define custom operations in the class itself.
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator+, +);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator-, -);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator*, *);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator/, /);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator==, ==);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator!=, !=);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator&&, &&);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator||, ||);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator<, <);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator>, >);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator<=, <=);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator>=, >=);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator&, &);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator|, |);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator^, ^);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator<<, <<);
    DEFINE_OPERATOR_VAR_VAL_BINARY(operator>>, >>);
    DEFINE_OPERATOR_VAR_VAL_UNARY(operator!, !);

    /// Writes the underlying value to the given memory reference.
    /// We call the operator= after the cast to the underlying type.
    void writeToMemory(const nautilus::val<int8_t*>& memRef) const;

protected:
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    ScalarVarVal(const detail::var_val_t t) : value(std::move(t)) { }

    detail::var_val_t value;
};

ScalarVarVal::ScalarVarVal(const ScalarVarVal& other) : value(other.value)
{
}

ScalarVarVal::ScalarVarVal(ScalarVarVal&& other) noexcept : value(std::move(other.value))
{
}

ScalarVarVal& ScalarVarVal::operator=(const ScalarVarVal& other)
{
    value = other.value;
    return *this;
}

ScalarVarVal& ScalarVarVal::operator=(ScalarVarVal&& other)
{
    value = std::move(other.value);
    return *this;
}

class ScalarVarValPtr
{
    detail::var_val_ptr_t value;

public:
    template <typename T>
    ScalarVarValPtr(const nautilus::val<T> t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_ptr_t>::value)
        : value(t)
    {
    }

    nautilus::val<bool> operator==(const ScalarVarValPtr& other) const { return this->cast<void>() == other.cast<void>(); }

    ScalarVarValPtr advanceBytes(nautilus::val<ssize_t> bytes) const
    {
        return std::visit(
            [&]<typename T0>(T0& value) { return ScalarVarValPtr(static_cast<T0>(static_cast<nautilus::val<int8_t*>>(value) + bytes)); },
            this->value);
    }

    ScalarVarVal operator*()
    {
        return std::visit([]<typename T0>(T0& value) { return ScalarVarVal(nautilus::val<typename T0::ValType>(*value)); }, this->value);
    }

    template <typename T>
    nautilus::val<T*> uncheckedCast() const
    {
        return std::visit([](auto&& x) { return static_cast<nautilus::val<T*>>(x); }, this->value);
    }

    template <typename T1>
    nautilus::val<T1*> cast() const
    {
        if (std::holds_alternative<nautilus::val<T1*>>(value))
        {
            return std::get<nautilus::val<T1*>>(value);
        }
        auto strType = std::visit([](auto&& x) -> decltype(auto) { return typeName<decltype(x)>(); }, value);
        throw UnsupportedOperation("Tried to cast to {} but was type {}", typeName<T1>(), strType);
    }

    template <typename T1>
    requires(std::same_as<void, T1>)
    nautilus::val<void*> cast() const
    {
        return std::visit([](auto&& x) { return static_cast<nautilus::val<void*>>(x); }, this->value);
    }
};


ScalarVarVal::operator bool() const
{
    return std::visit(
        []<typename T>(T& val) -> nautilus::val<bool>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool>(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
                /// As we will then compare val == 1, which will always be false.
                return !(val == nautilus::val<bool>(false));
            }
            else
            {
                throw UnsupportedOperation();
                return nautilus::val<bool>(false);
            }
        },
        value);
}

static_assert(!std::is_default_constructible_v<ScalarVarVal>, "Should not be default constructible");
static_assert(std::is_constructible_v<ScalarVarVal, int32_t>, "Should be constructible from int32_t");
static_assert(std::is_constructible_v<ScalarVarVal, nautilus::val<uint32_t>>, "Should be constructible from nautilus::val<uint32_t>");
static_assert(
    std::is_convertible_v<nautilus::val<uint32_t>, ScalarVarVal>, "Should allow conversion from nautilus::val<uint32_t> to ScalarVarVal");
static_assert(!std::is_convertible_v<int32_t, ScalarVarVal>, "Should not allow conversion from underlying to ScalarVarVal");

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const ScalarVarVal& varVal);

class FixedSizeVal
{
public:
    FixedSizeVal(std::vector<ScalarVarVal> values) : values(values), size(values.size()) { }
    std::vector<ScalarVarVal> values;
    size_t size;
    nautilus::val<bool> operator==(const FixedSizeVal& other) const
    {
        INVARIANT(this->size == other.size);
        return this->values == other.values;
    }
};

class VariableSizeVal
{
public:
    VariableSizeVal(ScalarVarValPtr data, nautilus::val<size_t> size, nautilus::val<size_t> numberOfElements)
        : data(std::move(data)), size(std::move(size)), numberOfElements(std::move(numberOfElements))
    {
    }
    ScalarVarValPtr data;
    nautilus::val<size_t> size;
    nautilus::val<size_t> numberOfElements;

    nautilus::val<bool> operator==(const VariableSizeVal& other) const
    {
        return this->numberOfElements == other.numberOfElements && this->size == other.size
            && nautilus::memcmp(data.cast<void>(), other.data.cast<void>(), this->size) == 0;
    }
};

class OptionalVal
{
    nautilus::val<bool> valid;
    ScalarVarVal value;
};

struct VarVal
{
    VarVal(ScalarVarVal scalar) : value(std::move(scalar)) { }
    VarVal(FixedSizeVal fixed_size_val) : value(std::move(fixed_size_val)) { }
    VarVal(VariableSizeVal fixed_size_val) : value(std::move(fixed_size_val)) { }

    ScalarVarVal& assumeScalar() { return std::get<ScalarVarVal>(value); }
    FixedSizeVal& assumeFixedSize() { return std::get<FixedSizeVal>(value); }
    VariableSizeVal& assumeVariableSize() { return std::get<VariableSizeVal>(value); }

    nautilus::val<bool> operator==(const VarVal& other) const
    {
        return std::visit(
                   []<typename T0, typename T1>(T0& lhs, T1& rhs) -> ScalarVarVal
                   {
                       if constexpr (!std::is_same_v<T0, T1>)
                       {
                           throw std::runtime_error("Type mismatch");
                       }
                       else
                       {
                           return lhs == rhs;
                       }
                   },
                   this->value,
                   other.value)
            .cast<nautilus::val<bool>>();
    }

    using impl = std::variant<ScalarVarVal, FixedSizeVal, VariableSizeVal>;
    impl value;
};
}


namespace NES
{
template <typename T>
concept PhysicalScalarTypeConcept = requires(const T& t) {
    { t.size() } -> std::same_as<size_t>;
    { t.alignment() } -> std::same_as<size_t>;
};

struct UInt64
{
    [[nodiscard]] size_t size() const { return 8; }
    [[nodiscard]] size_t alignment() const { return 8; }
};

struct UInt8
{
    [[nodiscard]] size_t size() const { return 1; }
    [[nodiscard]] size_t alignment() const { return 1; }
};

struct Int8
{
    [[nodiscard]] size_t size() const { return 1; }
    [[nodiscard]] size_t alignment() const { return 1; }
};

struct Char
{
    [[nodiscard]] size_t size() const { return 1; }
    [[nodiscard]] size_t alignment() const { return 1; }
};

static_assert(PhysicalScalarTypeConcept<UInt64>);

class PhysicalScalarType
{
public:
    size_t size() const
    {
        return std::visit([](auto inner) { return inner.size(); }, value);
    }

    size_t alignment() const
    {
        return std::visit([](auto inner) { return inner.alignment(); }, value);
    }

    using impl = std::variant<UInt64, UInt8, Int8, Char>;
    impl value;
};

struct Scalar
{
    [[nodiscard]] size_t size() const { return inner.size(); }
    [[nodiscard]] size_t alignment() const { return inner.alignment(); }

    PhysicalScalarType inner;
};

struct FixedSize
{
    [[nodiscard]] size_t size() const { return inner.size() * numberOfElements; }
    [[nodiscard]] size_t alignment() const { return inner.alignment(); }
    FixedSize(PhysicalScalarType inner, size_t number_of_elements) : inner(inner), numberOfElements(number_of_elements) { }
    PhysicalScalarType inner;
    size_t numberOfElements;
};

struct VariableSize
{
    [[nodiscard]] size_t size() const { return 4; }
    [[nodiscard]] size_t alignment() const { return 4; }
    VariableSize(PhysicalScalarType inner) : inner(inner) { }
    PhysicalScalarType inner;
};

class PhysicalType
{
public:
    size_t size() const
    {
        return std::visit([](auto inner) { return inner.size(); }, value);
    }

    size_t alignment() const
    {
        return std::visit([](auto inner) { return inner.alignment(); }, value);
    }

    using impl = std::variant<Scalar, FixedSize, VariableSize>;
    impl value;
};

struct PhysicalSchema
{
    std::vector<std::pair<FieldName, PhysicalType>> fields;
};

class MemoryRecordAccessor
{
public:
    virtual ~MemoryRecordAccessor() = default;

    virtual Nautilus::VarVal operator[](const FieldName& fieldName) = 0;
    virtual Nautilus::VarVal operator[](size_t index) = 0;
    void write(const FieldName& fieldName, Nautilus::ScalarVarVal scalar, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        write(fieldName, Nautilus::VarVal(scalar), bufferProvider);
    }
    void write(size_t index, Nautilus::ScalarVarVal scalar, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        write(index, Nautilus::VarVal(scalar), bufferProvider);
    }
    virtual void write(const FieldName& fieldName, Nautilus::VarVal, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) = 0;
    virtual void write(size_t index, Nautilus::VarVal, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) = 0;
    virtual void advance(nautilus::val<size_t> index) = 0;
};

template <typename T>
struct PhysicalNautilusMapping
{
};

#define ADD_MAPPING(Physical, NautilusT) \
    template <> \
    struct PhysicalNautilusMapping<Physical> \
    { \
        using NautilusType = nautilus::val<NautilusT>; \
        using PointerType = NautilusT*; \
        using BasicType = NautilusT; \
    }

ADD_MAPPING(UInt64, uint64_t);
ADD_MAPPING(UInt8, uint8_t);
ADD_MAPPING(Int8, int8_t);
ADD_MAPPING(Char, char);

struct TupleBufferRef
{
    template <typename T>
    nautilus::val<T*> getBuffer()
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getBuffer<T>(); }, tb);
    }

    nautilus::val<size_t> getBufferSize() const
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getBufferSize(); }, tb);
    }

    void setNumberOfTuples(nautilus::val<size_t> newNumberOfTuples) const
    {
        return nautilus::invoke(
            +[](Memory::TupleBuffer* tb, size_t newNumberOfTuples) { return tb->setNumberOfTuples(newNumberOfTuples); },
            tb,
            newNumberOfTuples);
    }

    nautilus::val<size_t> getNumberOfTuples() const
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getNumberOfTuples(); }, tb);
    }

    std::tuple<nautilus::val<unsigned>, Nautilus::ScalarVarValPtr, nautilus::val<size_t>>
    allocateChildBuffer(PhysicalScalarType scalarType, nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        return std::visit(
            [&]<typename T>(T)
            {
                auto [index, ptr, size] = allocateChildBuffer<typename PhysicalNautilusMapping<T>::BasicType>(provider);
                return std::make_tuple(index, Nautilus::ScalarVarValPtr(ptr), size);
            },
            scalarType.value);
    }

    template <typename T>
    std::tuple<nautilus::val<unsigned>, nautilus::val<T*>, nautilus::val<size_t>>
    allocateChildBuffer(nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        auto index = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, Memory::AbstractBufferProvider* provider)
            {
                auto buffer = provider->getBufferBlocking();
                return tb->storeChildBuffer(buffer);
            },
            tb,
            provider);
        auto size = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index) { return tb->loadChildBuffer(index).getBufferSize(); }, tb, index);
        auto data = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index) { return tb->loadChildBuffer(index).getBuffer<T>(); }, tb, index);
        return {index, data, size};
    }

    std::tuple<Nautilus::ScalarVarValPtr, nautilus::val<size_t>>
    loadChildBuffer(PhysicalScalarType scalarType, nautilus::val<unsigned> index)
    {
        return std::visit(
            [&]<typename T>(T)
            {
                auto [ptr, size] = loadChildBuffer<typename PhysicalNautilusMapping<T>::BasicType>(index);
                return std::make_tuple(Nautilus::ScalarVarValPtr(ptr), size);
            },
            scalarType.value);
    }

    template <typename T>
    std::tuple<nautilus::val<T*>, nautilus::val<size_t>> loadChildBuffer(nautilus::val<unsigned> index)
    {
        auto size = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index)
            {
                auto buffer = tb->loadChildBuffer(index);
                return buffer.getBufferSize();
            },
            tb,
            index);

        auto data = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index)
            {
                auto buffer = tb->loadChildBuffer(index);
                return buffer.getBuffer<T>();
            },
            tb,
            index);

        return {data, size};
    }

    explicit TupleBufferRef(nautilus::val<Memory::TupleBuffer*> tb) : tb(std::move(tb)) { }

private:
    nautilus::val<Memory::TupleBuffer*> tb;
};

namespace RowLayout
{
template <typename T>
struct MemoryLayout
{
};

#define ScalarTypeLayout(Physical) \
    template <> \
    struct MemoryLayout<Physical> \
    { \
        static NES::Nautilus::VarVal load(const Physical&, nautilus::val<int8_t*> address) \
        { \
            return Nautilus::ScalarVarVal( \
                PhysicalNautilusMapping<Physical>::NautilusType{ \
                    *static_cast<nautilus::val<typename PhysicalNautilusMapping<Physical>::NautilusType::raw_type*>>(address)}); \
        } \
        static void store(const Physical&, nautilus::val<int8_t*> address, NES::Nautilus::VarVal v) \
        { \
            *static_cast<nautilus::val<typename PhysicalNautilusMapping<Physical>::NautilusType::raw_type*>>(address) \
                = v.assumeScalar().cast<PhysicalNautilusMapping<Physical>::NautilusType>(); \
        } \
    }

ScalarTypeLayout(UInt8);
ScalarTypeLayout(Int8);
ScalarTypeLayout(Char);
ScalarTypeLayout(UInt64);

Nautilus::VarVal load(const PhysicalScalarType& pst, nautilus::val<int8_t*> address)
{
    return std::visit([&]<typename T>(const T& t) -> Nautilus::VarVal { return MemoryLayout<T>::load(t, address); }, pst.value);
}
void store(const PhysicalScalarType& pst, nautilus::val<int8_t*> address, Nautilus::VarVal v)
{
    std::visit([&]<typename T>(const T& t) { MemoryLayout<T>::store(t, address, v); }, pst.value);
}

template <>
struct MemoryLayout<Scalar>
{
    static NES::Nautilus::VarVal load(const Scalar& scalar, nautilus::val<int8_t*> address, TupleBufferRef)
    {
        return RowLayout::load(scalar.inner, address);
    }
    static void store(
        const Scalar& scalar,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef,
        nautilus::val<Memory::AbstractBufferProvider*>)
    {
        RowLayout::store(scalar.inner, address, v);
    }
};

template <>
struct MemoryLayout<FixedSize>
{
    static NES::Nautilus::VarVal load(const FixedSize& fixedSize, nautilus::val<int8_t*> address, TupleBufferRef)
    {
        std::vector<Nautilus::ScalarVarVal> values;
        values.reserve(fixedSize.numberOfElements);
        for (nautilus::static_val<size_t> i = 0; i < fixedSize.numberOfElements; i++)
        {
            values.emplace_back(RowLayout::load(fixedSize.inner, address).assumeScalar());
            address += fixedSize.inner.size();
        }
        return Nautilus::VarVal(Nautilus::FixedSizeVal(std::move(values)));
    }

    static void store(
        const FixedSize& fixedSize,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef,
        nautilus::val<Memory::AbstractBufferProvider*>)
    {
        auto& fixedSizeVal = v.assumeFixedSize();
        for (nautilus::static_val<size_t> i = 0; i < fixedSize.numberOfElements; i++)
        {
            RowLayout::store(fixedSize.inner, address, fixedSizeVal.values[i]);
            address += fixedSize.inner.size();
        }
    }
};

template <>
struct MemoryLayout<VariableSize>
{
    static NES::Nautilus::VarVal load(const VariableSize& type, nautilus::val<int8_t*> address, TupleBufferRef tb)
    {
        std::vector<Nautilus::ScalarVarVal> values;
        auto index = nautilus::val<unsigned>(*static_cast<nautilus::val<uint32_t*>>(address));
        auto [buffer, size] = tb.loadChildBuffer(type.inner, index);
        auto actualVariableSize = nautilus::val<unsigned>(*buffer.uncheckedCast<uint32_t>());
        return Nautilus::VarVal(
            Nautilus::VariableSizeVal(buffer.advanceBytes(4), actualVariableSize * type.inner.size(), actualVariableSize));
    }

    static void store(
        const VariableSize&,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef tb,
        nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        auto& variableSize = v.assumeVariableSize();
        auto [index, buffer, size] = tb.allocateChildBuffer<int8_t>(provider);
        *static_cast<nautilus::val<uint32_t*>>(address) = index;
        *static_cast<nautilus::val<uint32_t*>>(buffer) = variableSize.numberOfElements;
        buffer += 4;
        nautilus::memcpy(buffer, variableSize.data.cast<void>(), variableSize.size);
    }
};

void store(
    const PhysicalType& pst,
    nautilus::val<int8_t*> address,
    Nautilus::VarVal v,
    TupleBufferRef tb,
    nautilus::val<Memory::AbstractBufferProvider*> provider)
{
    std::visit([&]<typename T>(const T& t) { MemoryLayout<T>::store(t, address, v, tb, provider); }, pst.value);
}

Nautilus::VarVal load(const PhysicalType& pst, nautilus::val<int8_t*> address, TupleBufferRef tb)
{
    return std::visit([&]<typename T>(const T& t) -> Nautilus::VarVal { return MemoryLayout<T>::load(t, address, tb); }, pst.value);
}
}

class RowMemoryRecordAccessor : public MemoryRecordAccessor
{
    struct RowLayoutSchema
    {
        RowLayoutSchema(const PhysicalSchema& schema)
        {
            tupleSize = 0;
            for (size_t index = 0; const auto& [fieldName, type] : schema.fields)
            {
                indexes[fieldName] = index++;
                tupleOffset.push_back(tupleSize);
                physicalTypes.push_back(type);
                tupleSize += type.size();
            }
        }
        size_t tupleSize;
        std::unordered_map<FieldName, size_t> indexes;
        std::vector<size_t> tupleOffset;
        std::vector<PhysicalType> physicalTypes;
    };

public:
    RowMemoryRecordAccessor(TupleBufferRef tb, const PhysicalSchema& schema)
        : layout(schema), tb(tb), base(tb.getBuffer<int8_t>()), current(base)
    {
    }

    Nautilus::VarVal operator[](const FieldName& fieldName) override { return operator[](layout.indexes[fieldName]); }
    Nautilus::VarVal operator[](size_t index) override
    {
        auto address = current + layout.tupleOffset[index];
        return RowLayout::load(layout.physicalTypes[index], address, tb);
    }
    void write(const FieldName& fieldName, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) override
    {
        write(layout.indexes[fieldName], v, bufferProvider);
    }
    void write(size_t index, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) override
    {
        auto address = current + layout.tupleOffset[index];
        RowLayout::store(layout.physicalTypes[index], address, v, tb, bufferProvider);
    }
    void advance(nautilus::val<size_t> index) override { current = base + index * layout.tupleSize; }

    RowLayoutSchema layout;
    TupleBufferRef tb;
    nautilus::val<int8_t*> base;
    nautilus::val<int8_t*> current;
};

void fail()
{
    INVARIANT(false, "Something has failed");
}

class MemoryAccessor
{
public:
    explicit MemoryAccessor(TupleBufferRef tb, PhysicalSchema schema) : buffer(tb), schema(std::move(schema)) { }

    MemoryRecordAccessor& operator[](nautilus::val<size_t> index)
    {
        accessor->advance(index);
        return *accessor;
    }

    void append(std::vector<Nautilus::VarVal> fields, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        auto index = buffer.getNumberOfTuples();
        buffer.setNumberOfTuples(index + 1);
        accessor->advance(index);
        for (nautilus::static_val<size_t> i = 0; i < schema.fields.size(); ++i)
        {
            accessor->write(i, fields[i], bufferProvider);
        }
    }

    std::vector<Nautilus::VarVal> at(nautilus::val<size_t> index, std::unordered_set<FieldName> projection)
    {
        std::vector<Nautilus::VarVal> result;
        result.reserve(schema.fields.size());
        accessor->advance(index);
        for (nautilus::static_val<size_t> i = 0; i < schema.fields.size(); ++i)
        {
            if (!projection.empty() && !projection.contains(schema.fields[i].first))
            {
                continue;
            }

            result.emplace_back((*accessor)[i]);
        }
        return result;
    }

    std::vector<Nautilus::VarVal> at(nautilus::val<size_t> index) { return at(index, {}); }

    TupleBufferRef buffer;
    PhysicalSchema schema;
    std::unique_ptr<MemoryRecordAccessor> accessor = std::make_unique<RowMemoryRecordAccessor>(buffer, schema);
};

class MemoryLayoutTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("MemoryLayoutTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MemoryLayoutTest test class.");
    }
};

std::string generate_hexdump(const unsigned char* data, size_t length, size_t bytes_per_line = 16)
{
    std::stringstream hexdump;
    hexdump << std::hex << std::setfill('0');

    for (size_t i = 0; i < length; i += bytes_per_line)
    {
        // Print offset
        hexdump << std::setw(8) << i << ": ";

        // Print hex representation
        for (size_t j = 0; j < bytes_per_line; ++j)
        {
            if (i + j < length)
            {
                hexdump << std::setw(2) << static_cast<int>(data[i + j]) << " ";
            }
            else
            {
                hexdump << "   ";
            }
        }

        hexdump << " |";

        // Print ASCII representation
        for (size_t j = 0; j < bytes_per_line; ++j)
        {
            if (i + j < length)
            {
                unsigned char ch = data[i + j];
                hexdump << (std::isprint(ch) ? static_cast<char>(ch) : '.');
            }
        }

        hexdump << "|\n";
    }

    return hexdump.str();
}

TEST_F(MemoryLayoutTest, IsThisWorking)
{
    auto bm = Memory::BufferManager::create();
    PhysicalSchema schema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}})}};
    auto buffer = bm->getBufferBlocking();

    TupleBufferRef tb(std::addressof(buffer));
    MemoryAccessor accessor(tb, schema);

    auto arrayVal = Nautilus::FixedSizeVal{
        {nautilus::val<uint8_t>(1), nautilus::val<uint8_t>(2), nautilus::val<uint8_t>(3), nautilus::val<uint8_t>(4)}};

    accessor[0].write(FieldName("A"), Nautilus::ScalarVarVal{nautilus::val<uint64_t>(1)}, bm.get());
    accessor[0].write(FieldName("B"), arrayVal, bm.get());

    accessor[1].write(FieldName("A"), Nautilus::ScalarVarVal{nautilus::val<uint64_t>(2)}, bm.get());
    accessor[2].write(FieldName("A"), Nautilus::ScalarVarVal{nautilus::val<uint64_t>(3)}, bm.get());

    EXPECT_EQ(accessor[1][FieldName("A")].assumeScalar(), Nautilus::ScalarVarVal(nautilus::val<uint64_t>(2)));
    EXPECT_EQ(accessor[0][FieldName("B")].assumeFixedSize().values[2], Nautilus::ScalarVarVal(nautilus::val<uint64_t>(3)));

    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), buffer.getBufferSize());
}

TEST_F(MemoryLayoutTest, DoesItCompile)
{
    auto bm = Memory::BufferManager::create();
    PhysicalSchema schema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("C"), PhysicalType{VariableSize{PhysicalScalarType{Char{}}}})}};


    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    nautilus::engine::NautilusEngine engine(options);
    auto fn = engine.registerFunction(
        std::function(
            [&schema](
                nautilus::val<NES::Memory::TupleBuffer*> buffer,
                nautilus::val<Memory::AbstractBufferProvider*> provider,
                nautilus::val<char*> c,
                nautilus::val<size_t> length)
            {
                TupleBufferRef tb(buffer);
                MemoryAccessor accessor(tb, schema);
                Nautilus::VariableSizeVal var{c, length, length};

                auto arrayVal = Nautilus::FixedSizeVal{
                    {nautilus::val<uint8_t>(1), nautilus::val<uint8_t>(2), nautilus::val<uint8_t>(3), nautilus::val<uint8_t>(4)}};

                accessor[0].write(FieldName("A"), nautilus::val<uint64_t>(1), provider);
                accessor[0].write(FieldName("B"), arrayVal, provider);
                accessor[0].write(FieldName("C"), var, provider);

                auto record = accessor.at(0);
                accessor.append(record, provider);
                accessor.append(record, provider);
                accessor.append(record, provider);

                accessor[1].write(FieldName("A"), nautilus::val<uint64_t>(2), provider);
                accessor[2].write(FieldName("A"), nautilus::val<uint64_t>(3), provider);


                if (accessor[2][FieldName("C")] == accessor[1][FieldName("C")])
                {
                    nautilus::invoke(
                        +[](char* message) { NES_INFO("{}", std::string_view(message)); },
                        accessor[2][FieldName("C")].assumeVariableSize().data.cast<char>());
                    return accessor[2][FieldName("C")].assumeVariableSize().data.cast<char>();
                }

                return accessor[0][FieldName("C")].assumeVariableSize().data.cast<char>();
            }));

    auto buffer = bm->getBufferBlocking();
    std::string s = "I am a long string";
    char* c = fn(std::addressof(buffer), bm.get(), s.data(), s.size() + 1);
    std::cout << c << std::endl;
    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), buffer.getBufferSize());
}


template <typename T>
Nautilus::VarVal scalar(T t)
{
    return Nautilus::ScalarVarVal(nautilus::val<T>(t));
}

template <typename T>
Nautilus::VarVal fixedSize(std::initializer_list<T> ts)
{
    std::vector<Nautilus::ScalarVarVal> nautilusTs;
    for (const auto& t : ts)
    {
        nautilusTs.emplace_back(nautilus::val<T>(t));
    }
    return Nautilus::VarVal(Nautilus::FixedSizeVal(nautilusTs));
}

template <typename... T>
Nautilus::VarVal fixedSize(T... t)
{
    return fixedSize({t...});
}

template <typename T>
Nautilus::VarVal variableSize(T* t, size_t n)
{
    return Nautilus::VariableSizeVal(Nautilus::ScalarVarValPtr(nautilus::val<T*>(t)), n, n * sizeof(T));
}

TEST_F(MemoryLayoutTest, DoesItCompile2)
{
    auto bm = Memory::BufferManager::create();
    PhysicalSchema inputSchema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("C"), PhysicalType{VariableSize{PhysicalScalarType{Char{}}}})}};

    PhysicalSchema outputSchema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 5}})}};

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    nautilus::engine::NautilusEngine engine(options);


    auto project = engine.registerFunction(
        std::function(
            [&inputSchema, &outputSchema](
                nautilus::val<NES::Memory::TupleBuffer*> inputBuffer,
                nautilus::val<Memory::AbstractBufferProvider*> provider,
                nautilus::val<NES::Memory::TupleBuffer*> outputBuffer)
            {
                TupleBufferRef inputBufferRef(inputBuffer);
                MemoryAccessor inputAccessor(inputBufferRef, inputSchema);

                TupleBufferRef outputBufferRef(outputBuffer);
                MemoryAccessor outputAccessor(outputBufferRef, outputSchema);

                for (nautilus::val<size_t> i = 0; i < inputBufferRef.getNumberOfTuples(); i++)
                {
                    auto record = inputAccessor.at(i, {FieldName("B")});
                    outputAccessor.append(record, provider);
                }
            }));

    auto buffer = bm->getBufferBlocking();
    auto outputBuffer = bm->getBufferBlocking();

    MemoryAccessor inputAccessor(TupleBufferRef(std::addressof(buffer)), inputSchema);

    std::string s = "I am a long string";
    inputAccessor.append({scalar<uint64_t>(1), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());
    inputAccessor.append({scalar<uint64_t>(2), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());
    inputAccessor.append({scalar<uint64_t>(3), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());


    project(std::addressof(buffer), bm.get(), std::addressof(outputBuffer));

    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), 64);
    std::cout << generate_hexdump(outputBuffer.getBuffer<unsigned char>(), 64);
}
}