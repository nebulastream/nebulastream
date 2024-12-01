//
// Created by ls on 11/30/24.
//

#pragma once
#include <cstddef>
#include <unordered_set>
#include <variant>
#include <API/Schema.hpp>


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
}