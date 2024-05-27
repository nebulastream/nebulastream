//
// Created by veronika on 20.05.24.
//

#include <StatisticCollection/StatisticStorage/BPlusTreeWithBucketing/Node.hpp>

namespace NES::Statistic {

// Constructor implementation
Node::Node(std::vector<StatisticHash>& Keys,
           std::vector<std::vector<Bucket>>& Values,
           std::vector<Node>& Children,
           const uint64_t Capacity,
           bool leafnode){
    this->Keys = Keys;
    this->Values = Values;
    this->Children = Children;
    this->Capacity = Capacity;
    this->leafnode = leafnode;
    assert(Keys.size() <= Capacity);
    assert(Values.size() <= Capacity);
    assert(Children.size() <= Capacity);
   // assert(!(leafnode && !Children.empty()) // leaf node cannot have children
}

Node::Node(const uint64_t Capacity, bool leafnode)
    : Capacity(Capacity), leafnode(leafnode) {}

// Setters and Getters

std::vector<StatisticHash> Node::getKeys() { return Keys; }
void Node::setKeys(std::vector<StatisticHash>& NewKeys) { Keys = NewKeys; }

std::vector<Node> Node::getChildren() { return Children; }
void Node::setChildren(std::vector<Node>& NewChildren) { this->Children = NewChildren; }

std::vector<std::vector<Bucket>> Node::getValues() { return Values; }

void Node::setValues(std::vector<std::vector<Bucket>>& newValues) {
    this->Values = newValues;

}

bool Node::isLeafnode() { return leafnode; }
void Node::setLeafNode(bool leafnode) { this->leafnode = leafnode; }


std::vector<Bucket> Node::getValueOfKey(Node& node, const StatisticHash& key, size_t & index){
    for(size_t i = 0; i < node.getKeys().size(); ++i){
        if(node.getKeys()[i] == key){
            index = i;
            return node.getValues()[i];
        }
    }
    return {};
}
bool Node::operator==(const Node& other) { return this->Keys == other.Keys; }

}

