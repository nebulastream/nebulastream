//
// Created by veronika on 20.05.24.
//
#include "StatisticCollection/StatisticStorage/BPlusTreeWithBucketing/BPlusTreeWithBucketing.hpp"
#include "StatisticCollection/StatisticStorage/BPlusTreeWithBucketing/Node.hpp"
#include <deque>

#include "Measures/TimeMeasure.hpp"

namespace NES::Statistic {

StatisticStorePtr BPlusTreeWithBucketing::create(uint64_t& nodeCapacity, uint64_t& rangeSize) {
    // create an instance of the tree
    StatisticStorePtr treePtr = std::make_shared<BPlusTreeWithBucketing>();
    Node rootNode = Node(nodeCapacity,true);
    // init additional variables
    auto* tree = dynamic_cast<BPlusTreeWithBucketing*>(treePtr.get());
    if (tree) {
        tree->Capacity = nodeCapacity;
        tree->root = rootNode;
        tree->bucketsRangeSize = rangeSize;
        tree->allParents = {};
        // Initialize other additional variables as necessary
    }
    assert(nodeCapacity % 2 == 0);

    return treePtr;
}


// Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
std::vector<StatisticPtr> BPlusTreeWithBucketing::getStatistics(const StatisticHash& statisticHash,
                                                                const Windowing::TimeMeasure& startTs,
                                                                const Windowing::TimeMeasure& endTs) {
    std::vector<StatisticPtr> allStats;

    for(Node parent : allParents) {
        for (Node child : parent.getChildren()) {
            if (child.isLeafnode()) {
                for(StatisticHash key : child.getKeys()){
                    if(key == statisticHash){
                        size_t index;
                        std::vector<Bucket> bucketsForKey = child.getValueOfKey(child,key,index);
                        for(Bucket bucket : bucketsForKey){
                            if (bucket.getStartTs() >= startTs.getTime() && bucket.getEndTs() <= endTs.getTime()) {
                                for(StatisticPtr stat : bucket.GetStatistics()){
                                    allStats.push_back(stat);
                                }
                            }
                        }

                    }
                }
            }
        }
    }
    return allStats;
}

std::optional<StatisticPtr> BPlusTreeWithBucketing::getSingleStatistic(const StatisticHash& statisticHash,
                                                                       const Windowing::TimeMeasure& startTs,
                                                                       const Windowing::TimeMeasure& endTs) {
    for(Node parent : allParents) {
        for (Node child : parent.getChildren()) {
            if (child.isLeafnode()) {
                for(StatisticHash key : child.getKeys()){
                    if(key == statisticHash){
                        size_t index;
                        std::vector<Bucket> bucketsForKey = child.getValueOfKey(child,key,index);
                        for(Bucket bucket : bucketsForKey){
                            if (startTs.getTime() >= bucket.getStartTs() && endTs.getTime() <= bucket.getEndTs()) {
                                for(StatisticPtr stat : bucket.GetStatistics()){
                                    if(stat->getStartTs() == startTs && stat->getEndTs() == endTs){
                                        return stat;
                                    }
                                }
                            }
                        }

                    }
                }
            }
        }
    }
    return {};
}


bool BPlusTreeWithBucketing::insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) {


    Node leaf = searchNode(const_cast<StatisticHash&>(statisticHash),root);



    // case where the key already exists, and we just need to add the new statistic
    bool exists = std::binary_search(leaf.getKeys().begin(), leaf.getKeys().end(), statisticHash);
    if(exists) {
        return findOrCreateBucket(leaf, statistic, statisticHash);
    }

    // case where there is no space in the node and we have to split it
    if(leaf.getKeys().size() == Capacity){
        //create the first new node
        std::vector<StatisticHash> newKeys1;
        std::copy(leaf.getKeys().begin(), leaf.getKeys().begin() + Capacity / 2, std::back_inserter(newKeys1));
        std::vector<std::vector<Bucket>> newValues1;
        std::copy(leaf.getValues().begin(), leaf.getValues().begin() + Capacity / 2, std::back_inserter(newValues1));
        Node newNode1 = Node(Capacity, true);
        newNode1.setKeys(newKeys1);
        newNode1.setValues(newValues1);

        //create the second new node
        std::vector<StatisticHash> newKeys2;
        std::copy(leaf.getKeys().begin() + Capacity / 2, leaf.getKeys().begin() + Capacity, std::back_inserter(newKeys2));
        std::vector<std::vector<Bucket>> newValues2;
        std::copy(leaf.getValues().begin() + Capacity / 2, leaf.getValues().begin() + Capacity, std::back_inserter(newValues2));
        Node newNode2 = Node(Capacity, true);
        newNode2.setKeys(newKeys2);
        newNode2.setValues(newValues2);

        // key should be inserted in the second node
        if(statisticHash > newNode2.getKeys()[0]){
            // iterator that finds the right place of the new key in the node
            auto it = std::lower_bound(newNode2.getKeys().begin(), newNode2.getKeys().end(), statisticHash);
            newKeys2.insert(it, statisticHash);
            // insert new values
            findOrCreateBucket(newNode2,statistic,statisticHash);
            newNode2.setKeys(newKeys2);
        }else{
            auto it = std::lower_bound(newNode1.getKeys().begin(), newNode1.getKeys().end(), statisticHash);
            newKeys1.insert(it, statisticHash);
            // insert new values
            findOrCreateBucket(newNode1,statistic,statisticHash);
            newNode1.setKeys(newKeys1);
        }
        // case where root will be split => root becomes inner node with two children newNode1 and newNode2
        if(allParents.empty()){
            std::vector<StatisticHash> rootKeys;
            rootKeys.push_back(newKeys2[0]);
            std::vector<std::vector<Bucket>> rootValues;
            std::vector<Node> rootChildren;
            rootChildren.push_back(newNode1);
            rootChildren.push_back(newNode2);
            Node newRoot = Node(rootKeys,rootValues,rootChildren,Capacity,false);
            this->root = newRoot;
            allParents.push_back(root);

            return true;
            //case where the parent of the split node (leaf) has space for one more child
        }else if(getParent(leaf).getChildren().size() < Capacity){
            auto children = getParent(leaf).getChildren();
            auto it = std::find(children.begin(), children.end(), leaf);
            if (it != children.end()) {
                children.erase(it); // Remove the leaf node
                // Add the new nodes
                children.push_back(newNode1);
                children.push_back(newNode2);
            }
            // add a new key to the parent's keys
            std::vector<StatisticHash> parentKeys = getParent(leaf).getKeys();
            parentKeys.push_back(newNode2.getKeys()[0]);
            getParent(leaf).setKeys(parentKeys);

            return true;

            // case where the leaf node becomes an inner node with two children newNode1 and newNode2
        }else{
            std::vector<std::vector<Bucket>> emptyValues;
            leaf.setValues(emptyValues);
            std::vector<StatisticHash> newKeys;
            newKeys.push_back(newKeys2[0]);
            leaf.setKeys(newKeys);
            std::vector<Node> newChildren;
            newChildren.push_back(newNode1);
            newChildren.push_back(newNode2);
            leaf.setChildren(newChildren);
            leaf.setLeafNode(false);

            allParents.push_back(leaf);

            return true;

        }
    }
    // there is free space in the leaf node and there is no need to split it
    else if(leaf.getKeys().size() < Capacity){
        // find the right place and insert the new key
        auto it = std::lower_bound(leaf.getKeys().begin(), leaf.getKeys().end(), statisticHash);
        std::vector<StatisticHash> leafKeys = leaf.getKeys();
        leafKeys.insert(it,statisticHash);
        leaf.setKeys(leafKeys);
        findOrCreateBucket(leaf,statistic,statisticHash);  // insert the statistic to the right bucket

        return true;
    }
    return false;
}

bool BPlusTreeWithBucketing::deleteStatistics(const StatisticHash& statisticHash,
                                             const Windowing::TimeMeasure& startTs,
                                             const Windowing::TimeMeasure& endTs) {
    // find the node holding this key
    bool statFoundAndDeleted = false;
    Node leaf = searchNode(const_cast<StatisticHash&>(statisticHash),root);
    std::vector<std::vector<Bucket>> leafValues = leaf.getValues();

    for(size_t i = 0; i < leaf.getKeys().size(); i++){
        if(leaf.getKeys()[i] == statisticHash){
            for(Bucket bucket : leafValues[i]){
                if(bucket.getStartTs() >= startTs.getTime() && bucket.getEndTs() <= endTs.getTime())
                    for(StatisticPtr stat : bucket.GetStatistics()){
                        if(startTs <= stat->getStartTs() && stat->getEndTs() <= endTs){
                            bucket.deleteStatistic(stat);
                            statFoundAndDeleted = true;
                        }
                    }
            }

        }
    }
    return statFoundAndDeleted;
}

std::vector<HashStatisticPair> BPlusTreeWithBucketing::getAllStatistics() {
    //using HashStatisticPair = std::pair<StatisticHash, StatisticPtr>;
    std::vector<HashStatisticPair> returnStatisticsVector;

    for(Node parent : allParents){
        for(Node child : parent.getChildren()){
            if(child.isLeafnode()){
                for(size_t i = 0; i < child.getKeys().size(); i++){
                    StatisticHash key = child.getKeys()[i];
                    std::vector<Bucket> bucketsForKey = child.getValues()[i];
                    for(Bucket bucket : bucketsForKey){
                        for(StatisticPtr stat : bucket.GetStatistics()){
                            returnStatisticsVector.emplace_back(key, stat);
                        }
                    }
                }
            }
        }
    }
    return returnStatisticsVector;
}


// other methods that are not in abstract store

// This function finds the leaf node holding the key
Node BPlusTreeWithBucketing::searchNode(StatisticHash& statisticHash, Node& node){

    if(node.isLeafnode()){
        return node;
    }

    if(!allParents.empty()){
        allParents.push_back(node);
    }
    std::vector<Node> Children = node.getChildren();
    for (size_t i = 0; i < Children.size(); ++i) {
        Node child =Children[i];
        Node* rightSibling = nullptr;
        if(i+1 < Children.size()){
            rightSibling = &Children[i + 1];
        }
        if(rightSibling != nullptr && rightSibling->getKeys()[0] > statisticHash && child.getKeys()[0] <= statisticHash){
            if(child.isLeafnode()){
                return child;
            }
            searchNode(statisticHash,child);
        }else if(rightSibling == nullptr){
            if(child.isLeafnode()){
                return child;
            }
            searchNode(statisticHash,child);
        }

    }
    std::cout <<  "Can not find leaf. Something is wrong.";
    Node emptyNode = Node(Capacity,false);
    return emptyNode;
}

Node BPlusTreeWithBucketing::getParent(Node& node) {
    for(Node parent : allParents){
        for(Node child : parent.getChildren()){
            if(child.getKeys()[0] == node.getKeys()[0]){
                return parent;
            }
        }
    }
    return root;
}
// find the right bucket and insert statistics
bool BPlusTreeWithBucketing::findOrCreateBucket(Node& leaf, const StatisticPtr& statistic, const StatisticHash& statisticHash) const{

    if((statistic->getEndTs().getTime() - statistic->getStartTs().getTime() + 1) > bucketsRangeSize){
        std::cout << "Error: The statistic start and end times exceed the bucket size. Increase the bucket size.";
        return false;
    }
    // get the bucket vector corresponding to the key
    size_t index;
    std::vector<Bucket> bucketVector = leaf.getValueOfKey(leaf, statisticHash, index);

    // insert the stat in the bucket if it already exists
    for(Bucket& bucket : bucketVector) {
        if (statistic->getStartTs().getTime() >= bucket.getStartTs() && statistic->getEndTs().getTime() <= bucket.getEndTs()) {
            bucket.insertStatistic(statistic);
            return true;
        }
    }
    // or create a new one and insert the stat to it
    unsigned long start = (statistic->getStartTs().getTime() / bucketsRangeSize) * bucketsRangeSize;
    unsigned long end  = (statistic->getEndTs().getTime() / bucketsRangeSize) * bucketsRangeSize + (bucketsRangeSize -1);
    // Create bucket
    Bucket bucket = Bucket(start,end,bucketsRangeSize);
    // insert stat
    bucket.insertStatistic(statistic);
    bucketVector.push_back(bucket);
    return true;

}

BPlusTreeWithBucketing::~BPlusTreeWithBucketing() = default;



}
    







