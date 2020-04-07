#ifndef NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
#include <Nodes/Node.hpp>
#include <queue>
namespace NES {
class BSFIterator {
  public:
    NodePtr root;
    BSFIterator(NodePtr root) : root(root) {

    }
    // member typedefs provided through inheriting from std::iterator
    class iterator : public std::iterator<
        std::input_iterator_tag,   // iterator_category
        NodePtr,                      // value_type
        NodePtr,                      // difference_type
        const NodePtr,               // pointer
        NodePtr> {
        std::queue<NodePtr> currentStack;
      public:
        explicit iterator(NodePtr current) {
            currentStack.push(current);
        }
        iterator& operator++() {
            auto current = currentStack.top();
            currentStack.pop();
            for(auto child : current->getChildren()){
                currentStack.push(child);
            }

            return *this;
        }
        bool operator!=(const iterator& other) const {
            return !currentStack.empty();
        }
        reference operator*() const { return currentStack.top(); }
    };
    iterator begin() { return iterator(root); }
    iterator end() { return iterator(NodePtr()); }
};

}

#endif //NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
