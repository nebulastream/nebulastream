#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <utility>

namespace NES {

DepthFirstNodeIterator::DepthFirstNodeIterator(NodePtr start) : start(std::move(start)){};

DepthFirstNodeIterator::iterator DepthFirstNodeIterator::begin() { return iterator(start); }

DepthFirstNodeIterator::iterator DepthFirstNodeIterator::end() { return iterator(); }

DepthFirstNodeIterator::iterator::iterator(NodePtr current) {
    workStack.push(current);
}

DepthFirstNodeIterator::iterator::iterator() = default;

bool DepthFirstNodeIterator::iterator::operator!=(const iterator& other) const {
    // todo currently we only check if we reached the end of the iterator.
    if (workStack.empty() && other.workStack.empty()) {
        return false;
    };
    return true;
}

NodePtr DepthFirstNodeIterator::iterator::operator*() { return workStack.top(); }

DepthFirstNodeIterator::iterator& DepthFirstNodeIterator::iterator::operator++() {
    if (workStack.empty()) {
        NES_DEBUG("DF Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workStack.top();
        workStack.pop();
        for (auto child : current->getChildren()) {
            workStack.push(child);
        }
    }
    return *this;
}
}// namespace NES
