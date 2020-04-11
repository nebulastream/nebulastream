#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <utility>

namespace NES {

BreadthFirstNodeIterator::BreadthFirstNodeIterator(NodePtr start) : start(std::move(start)) {};

BreadthFirstNodeIterator::iterator BreadthFirstNodeIterator::begin() { return iterator(start); }

BreadthFirstNodeIterator::iterator BreadthFirstNodeIterator::end() { return iterator(); }

BreadthFirstNodeIterator::iterator::iterator(NodePtr current) {
    workQueue.push(current);
}

BreadthFirstNodeIterator::iterator::iterator() = default;

bool BreadthFirstNodeIterator::iterator::operator!=(const iterator& other) const {
    // todo currently we only check if we reached the end of the iterator.
    if(workQueue.empty() && other.workQueue.empty()){
        return false;
    };
    return true;
}

NodePtr BreadthFirstNodeIterator::iterator::operator*() { return workQueue.front(); }

BreadthFirstNodeIterator::iterator& BreadthFirstNodeIterator::iterator::operator++() {
    if (workQueue.empty()) {
        NES_DEBUG("BF Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workQueue.front();
        workQueue.pop();
        for (auto child : current->getChildren()) {
            workQueue.push(child);
        }
    }
    return *this;
}
}

