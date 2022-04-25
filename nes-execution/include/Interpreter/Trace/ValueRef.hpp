#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
#include <memory>
namespace NES::Interpreter{
enum OpCode { ADD, SUB, DIV, MUL, EQUALS, LESS_THAN, NEGATE, AND, OR, CMP, JMP, CONST, ASSIGN, RETURN, LOAD, STORE };


class ValueRef {
  public:
    ValueRef() : blockId(), operationId(){};
    ValueRef(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId){};
    ValueRef(const ValueRef& other) : blockId(other.blockId), operationId(other.operationId) {}
    ValueRef& operator=(const ValueRef& other) {
        this->operationId = other.operationId;
        this->blockId = other.blockId;
        return *this;
    }
    uint32_t blockId;
    uint32_t operationId;
    bool operator==(const ValueRef& rhs) const;
    bool operator!=(const ValueRef& rhs) const;
    friend std::ostream& operator<<(std::ostream& os, const ValueRef& tag);
};

struct ValueRefHasher {
    std::size_t operator()(const ValueRef& k) const {
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = hasher(k.operationId) ^ hasher(k.blockId);
        return hashVal;
    }
};

}

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
