//
// Created by Usama Bin Tariq on 23.09.24.
//

#pragma once
#include <string>
#include <stdexcept>
#include <cstring>
#include <iostream>

namespace nebula {
    //this class represent the constant values in the query like select a from b where c = 1, d = 'f'
    //this class will hold 1 as int and f as string
    class Value {
public:
    // Enum for value types
    enum class Type {
        Null,
        Bool,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Float,
        Double,
        String
    };

private:
    // Union to hold different types of values
    union Data {
        bool b;
        int32_t i32;
        uint32_t u32;
        int64_t i64;
        uint64_t u64;
        float f;
        double d;
        char* s;

        Data() {}
        ~Data() {}
    } data;

    Type type;

    void clear() {
        if (type == Type::String && data.s != nullptr) {
            delete[] data.s;
        }
    }

public:
    // Constructors for each type
    Value() : type(Type::Null) {} // Null
    Value(bool b) : type(Type::Bool) { data.b = b; }
    Value(int32_t i) : type(Type::Int32) { data.i32 = i; }
    Value(uint32_t u) : type(Type::UInt32) { data.u32 = u; }
    Value(int64_t i) : type(Type::Int64) { data.i64 = i; }
    Value(uint64_t u) : type(Type::UInt64) { data.u64 = u; }
    Value(float f) : type(Type::Float) { data.f = f; }
    Value(double d) : type(Type::Double) { data.d = d; }
    Value(const std::string& s) : type(Type::String) {
        data.s = new char[s.size() + 1];
        std::strcpy(data.s, s.c_str());
    }
    Value(const char* s) : type(Type::String) {
        data.s = new char[std::strlen(s) + 1];
        std::strcpy(data.s, s);
    }

    // Copy constructor
    Value(const Value& other) : type(other.type) {
        if (type == Type::String) {
            data.s = new char[std::strlen(other.data.s) + 1];
            std::strcpy(data.s, other.data.s);
        } else {
            data = other.data;
        }
    }

    // Assignment operator
    Value& operator=(const Value& other) {
        if (this != &other) {
            clear();
            type = other.type;
            if (type == Type::String) {
                data.s = new char[std::strlen(other.data.s) + 1];
                std::strcpy(data.s, other.data.s);
            } else {
                data = other.data;
            }
        }
        return *this;
    }

    // Destructor
    ~Value() {
        clear();
    }

    // Type getter
    Type getType() const { return type; }

    // Comparators (==, !=, <, >)
    bool operator==(const Value& other) const {
        if (type != other.type) return false;
        switch (type) {
            case Type::Null: return true;
            case Type::Bool: return data.b == other.data.b;
            case Type::Int32: return data.i32 == other.data.i32;
            case Type::UInt32: return data.u32 == other.data.u32;
            case Type::Int64: return data.i64 == other.data.i64;
            case Type::UInt64: return data.u64 == other.data.u64;
            case Type::Float: return data.f == other.data.f;
            case Type::Double: return data.d == other.data.d;
            case Type::String: return std::strcmp(data.s, other.data.s) == 0;
        }
        return false;
    }

    bool operator!=(const Value& other) const {
        return !(*this == other);
    }

    bool operator<(const Value& other) const {
        if (type != other.type) throw std::invalid_argument("Incompatible types");
        switch (type) {
            case Type::Bool: return data.b < other.data.b;
            case Type::Int32: return data.i32 < other.data.i32;
            case Type::UInt32: return data.u32 < other.data.u32;
            case Type::Int64: return data.i64 < other.data.i64;
            case Type::UInt64: return data.u64 < other.data.u64;
            case Type::Float: return data.f < other.data.f;
            case Type::Double: return data.d < other.data.d;
            case Type::String: return std::strcmp(data.s, other.data.s) < 0;
            default: throw std::invalid_argument("Null comparison");
        }
    }

    bool operator>(const Value& other) const {
        return other < *this;
    }

    // Cast functions
    bool asBool() const {
        if (type == Type::Bool) return data.b;
        throw std::bad_cast();
    }

    int32_t asInt32() const {
        if (type == Type::Int32) return data.i32;
        throw std::bad_cast();
    }

    uint32_t asUInt32() const {
        if (type == Type::UInt32) return data.u32;
        throw std::bad_cast();
    }

    int64_t asInt64() const {
        if (type == Type::Int64) return data.i64;
        throw std::bad_cast();
    }

    uint64_t asUInt64() const {
        if (type == Type::UInt64) return data.u64;
        throw std::bad_cast();
    }

    float asFloat() const {
        if (type == Type::Float) return data.f;
        throw std::bad_cast();
    }

    double asDouble() const {
        if (type == Type::Double) return data.d;
        throw std::bad_cast();
    }

    std::string asString() const {
        if (type == Type::String) return std::string(data.s);
        throw std::bad_cast();
    }
        std::string toString() const {
        switch (type) {
            case Type::Null:
                return "Null";
            case Type::Bool:
                return data.b ? "true" : "false";
            case Type::Int32:
                return std::to_string(data.i32);
            case Type::UInt32:
                return std::to_string(data.u32);
            case Type::Int64:
                return std::to_string(data.i64);
            case Type::UInt64:
                return std::to_string(data.u64);
            case Type::Float:
                return std::to_string(data.f);
            case Type::Double:
                return std::to_string(data.d);
            case Type::String:
                return std::string(data.s);
            default:
                throw std::logic_error("Unknown type");
        }
    }

    // Print function (for testing purposes)
    void print() const {
        switch (type) {
        case Type::Null:
            std::cout << "Null";
            break;
        case Type::Bool:
            std::cout << (data.b ? "true" : "false");
            break;
        case Type::Int32:
            std::cout << data.i32;
            break;
        case Type::UInt32:
            std::cout << data.u32;
            break;
        case Type::Int64:
            std::cout << data.i64;
            break;
        case Type::UInt64:
            std::cout << data.u64;
            break;
        case Type::Float:
            std::cout << data.f;
            break;
        case Type::Double:
            std::cout << data.d;
            break;
        case Type::String:
            std::cout << data.s;
            break;
        }
    }
};
}