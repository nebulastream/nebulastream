#include <Nodes/Util/Iterators/BFNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <utility>

namespace NES {

BFNodeIterator::BFNodeIterator(NodePtr start) : start(std::move(start)) {};

BFNodeIterator::iterator BFNodeIterator::begin() { return iterator(start); }

BFNodeIterator::iterator BFNodeIterator::end() { return iterator(); }

BFNodeIterator::iterator::iterator(NodePtr current) {
    workQueue.push(current);
}

BFNodeIterator::iterator::iterator() = default;

bool BFNodeIterator::iterator::operator!=(const iterator& other) const {
    // todo currently we only check if we reached the end of the iterator.
    if(workQueue.empty() && other.workQueue.empty()){
        return false;
    };
    return true;
}

NodePtr BFNodeIterator::iterator::operator*() { return workQueue.front(); }

BFNodeIterator::iterator& BFNodeIterator::iterator::operator++() {
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

