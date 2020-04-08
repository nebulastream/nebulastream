#ifndef NES_INCLUDE_NODES_UTIL_ITERATORS_DFNODEITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_ITERATORS_DFNODEITERATOR_HPP_
#include <stack>
#include <memory>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Depth-First iterator for node trees.
 * We first iterate over all children and then process nodes at the same level.
 */
class DFNodeIterator {
  public:
    explicit DFNodeIterator(NodePtr start);
    DFNodeIterator() = default;

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
      public:
        explicit iterator(NodePtr current);
        explicit iterator();

        /**
         * @brief Moves the iterator to the next node.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position.
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        NodePtr operator*();
      private:
        std::stack<NodePtr> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node.
     * @return iterator.
     */
    iterator begin();
    /**
     * @brief The end of this iterator has an empty work stack.
     * @return iterator.
     */
    iterator end();
  private:
    NodePtr start;
};
}

#endif //NES_INCLUDE_NODES_UTIL_ITERATORS_DFNODEITERATOR_HPP_
