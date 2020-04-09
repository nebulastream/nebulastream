#ifndef NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
#include <queue>
#include <memory>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Breadth-First iterator for node trees.
 * We first iterate over all records at the same level and then go to the next level.
 */
class BFNodeIterator {
  public:
    explicit BFNodeIterator(NodePtr start);
    BFNodeIterator() = default;

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
      public:
        explicit iterator(NodePtr current);
        explicit iterator();

        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        NodePtr operator*();
      private:
        std::queue<NodePtr> workQueue;
    };

    /**
     * @brief Starts a new iterator at the start node.
     * @return iterator
     */
    iterator begin();
    
    /**
    * @brief The end of this iterator has an empty work stack.
    * @return iterator
    */
    iterator end();
  private:
    NodePtr start;
};
}

#endif //NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
