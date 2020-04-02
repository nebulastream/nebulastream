#ifndef NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
#include <Nodes/Node.hpp>
namespace NES {
class DSFIterator {
  public:
    NodePtr root;
    DSFIterator(NodePtr root) : root(root) {

    }
    // member typedefs provided through inheriting from std::iterator
    class iterator : public std::iterator<
        std::input_iterator_tag,   // iterator_category
        NodePtr,                      // value_type
        NodePtr,                      // difference_type
        const NodePtr,               // pointer
        NodePtr                       // reference
    > {
        NodePtr current;
      public:
        explicit iterator(NodePtr current) : current(current) {}
        iterator& operator++() {
            if (current->getChildren().size() > 0) {
                current = current->getChildren()[0];
            }
            return *this;
        }
        bool operator!=(const iterator& other) const {
            return true;
        }
        reference operator*() const { return current; }
    };
    iterator begin() { return iterator(root); }
    iterator end() { return iterator(nullptr); }
};

}

#endif //NES_INCLUDE_NODES_UTIL_DFSITERATOR_HPP_
