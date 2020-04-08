#include <Nodes/Util/Iterators/DFNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <utility>

namespace NES {

DFNodeIterator::DFNodeIterator(NodePtr start) : start(std::move(start)) {};

DFNodeIterator::iterator DFNodeIterator::begin() { return iterator(start); }

DFNodeIterator::iterator DFNodeIterator::end() { return iterator(); }

DFNodeIterator::iterator::iterator(NodePtr current) {
    workStack.push(current);
}

DFNodeIterator::iterator::iterator() = default;

bool DFNodeIterator::iterator::operator!=(const iterator& other) const {
    if (workStack.empty() && other.workStack.empty()) {
        return false;
    };
    return true;
}

NodePtr DFNodeIterator::iterator::operator*() { return workStack.top(); }

DFNodeIterator::iterator& DFNodeIterator::iterator::operator++() {
    auto current = workStack.top();
    workStack.pop();
    for (auto child : current->getChildren()) {
        workStack.push(child);
    }
    return *this;
}
}

