#ifndef NES_INCLUDE_QUERYCOMPILER_IR_NODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_NODE_HPP_
#include <concepts>
#include <memory>
#include <typeindex>
#include <typeinfo>
#include <utility>
namespace NES::QueryCompilation::IR {

class Stamp {};

class Type {
  public:
    Type(std::type_index typeInfo) : typeInfo(typeInfo) {}

    template<class name>
    static Type const create() {
        return {std::type_index(typeid(Type))};
    }

    bool operator==(const Type& rhs) const { return this->typeInfo == rhs.typeInfo; };

  private:
    const std::type_index typeInfo;
};

class Node;

template<typename T>
concept IsNode = requires() {
    std::is_base_of_v<Type, T>;
};

template<typename T>
concept HasType = requires(T a) {
    IsNode<T>;
    { T::NodeType() } -> std::convertible_to<Type>;
};

class Node {
  public:
    explicit Node(const Type type) : type(type) {}

    template<HasType T>
    bool is_a() {
        return T::NodeType() == this->type;
    };

  private:
    const Type type;
};

template<IsNode T>
class NodeInfo {
  public:
    static Type NodeType() {
        const static auto type = Type::create<T>();
        return type;
    };
};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_NODE_HPP_
