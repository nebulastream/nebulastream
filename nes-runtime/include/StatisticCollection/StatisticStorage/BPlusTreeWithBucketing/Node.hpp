//
// Created by veronika on 20.05.24.
//

#ifndef NES_NODE_HPP
#define NES_NODE_HPP

#include "Bucket.hpp"
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <optional>
#include <cassert>

namespace NES::Statistic {
class Node;
class Statistic;
class Bucket;
using StatisticPtr = std::shared_ptr<Statistic>;
using HashStatisticPair = std::pair<StatisticHash, StatisticPtr>;

class Node{
    protected:
        std::vector<StatisticHash> Keys;
        std::vector<std::vector<Bucket>> Values;  // Each value is an array of buckets corresponding to the key
        std::vector<Node> Children;
        uint64_t Capacity;
        bool leafnode;

    public:
    // Constructor
        Node(std::vector<StatisticHash>& Keys, std::vector<std::vector<Bucket>>& Values, std::vector<Node>& Children, uint64_t Capacity, bool leafnode);
        Node(uint64_t Capacity, bool leafnode);

        std::vector<StatisticHash> getKeys();
        void setKeys(std::vector<StatisticHash>& NewKeys);

        std::vector<std::vector<Bucket>> getValues();
        void setValues(std::vector<std::vector<Bucket>>& NewValues);

        std::vector<Node> getChildren();
        void setChildren(std::vector<Node>& NewChildren);

        bool isLeafnode();
        void setLeafNode(bool leafnode);

        std::vector<Bucket>  getValueOfKey(Node& node, const StatisticHash& key, size_t& index);
       // bool isNodeUnderfilled(Node& node);
        bool operator==(const Node& other);

        virtual ~Node() = default;

};

}
#endif//NES_NODE_HPP