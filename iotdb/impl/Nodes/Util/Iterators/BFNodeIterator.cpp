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
    if(workQueue.empty() && other.workQueue.empty()){
        return false;
    };
    return true;
}

NodePtr BFNodeIterator::iterator::operator*() { return workQueue.front(); }

BFNodeIterator::iterator& BFNodeIterator::iterator::operator++() {
    auto current = workQueue.front();
    workQueue.pop();
    for (auto child : current->getChildren()) {
        workQueue.push(child);
    }
    return *this;
}
}

