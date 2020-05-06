#ifndef NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
#include <memory>
#include <queue>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Breadth-First iterator for node trees.
 * We first iterate over all records at the same level and then go to the next level.
 */
class BreadthFirstNodeIterator {
  public:
    explicit BreadthFirstNodeIterator(NodePtr start);

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
        friend class BreadthFirstNodeIterator;

      public:
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
        explicit iterator(NodePtr current);
        explicit iterator();
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
}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_BSFITERATOR_HPP_
