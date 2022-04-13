/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
        https://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef MLIR_APPROACH_NESABSTRACTIONIFNODE_HPP
#define MLIR_APPROACH_NESABSTRACTIONIFNODE_HPP

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionBinOpNode.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <vector>

class NESAbstractionIfNode : public NESAbstractionNode {
public:
    enum ComparisonType : unsigned {
        // Opcode                       Intuitive operation
        FLOAT_FALSE = 0,            /// Always false (always folded)
        FLOAT_ORDERED_EQ = 1,       /// True if ordered and equal
        FLOAT_ORDERED_GT = 2,       /// True if ordered and greater than
        FLOAT_ORDERED_GE = 3,       /// True if ordered and greater than or equal
        FLOAT_ORDERED_LT = 4,       /// True if ordered and less than
        FLOAT_ORDERED_LE = 5,       /// True if ordered and less than or equal
        FLOAT_ORDERED_NE = 6,       /// True if ordered and operands are unequal
        FLOAT_ORDERED_RD = 7,       /// True if ordered (no nans)
        FLOAT_ORDERED_NO = 8,       /// True if unordered: isnan(X) | isnan(Y)
        FLOAT_UNORDERED_EQ = 9,     /// True if unordered or equal
        FLOAT_UNORDERED_GT = 10,    /// True if unordered or greater than
        FLOAT_UNORDERED_GE = 11,    /// True if unordered, greater than, or equal
        FLOAT_UNORDERED_LT = 12,    /// True if unordered or less than
        FLOAT_UNORDERED_LE = 13,    /// True if unordered, less than, or equal
        FLOAT_UNORDERED_NE = 14,    /// True if unordered or not equal
        FLOAT_TRUE = 15,            /// Always true (always folded)

        INT_EQ = 32,                /// Equal
        INT_NE = 33,                /// Not equal
        UNSIGNED_INT_GT = 34,       /// Unsigned greater than
        UNSIGNED_INT_GE = 35,       /// Unsigned greater or equal
        UNSIGNED_INT_LT = 36,       /// Unsigned less than
        UNSIGNED_INT_LE = 37,       /// Unsigned less or equal
        SIGNED_INT_GT = 38,         /// Signed greater than
        SIGNED_INT_GE = 39,         /// Signed greater or equal
        SIGNED_INT_LT = 40,         /// Signed less than
        SIGNED_INT_LE = 41,         /// Signed less or equal
    };

    NESAbstractionIfNode(uint64_t nodeId, bool isRootNode, std::vector<ComparisonType> comparisonType, 
                         std::vector<int32_t> comparisonValues);
    ~NESAbstractionIfNode() override = default;

    std::vector<ComparisonType> getComparisons();

    std::vector<int32_t> getComparisonValues();

    static bool classof(const NESAbstractionNode *Node);


private:
    std::vector<ComparisonType> comparisons;
    std::vector<int32_t> comparisonValues;
//    std::vector<int32_t> comparisonValueTypes; //not used yet, all types are int32_t
    std::vector<std::shared_ptr<NESAbstractionNode>> childNodes;
};

#endif //MLIR_APPROACH_NESABSTRACTIONIFNODE_HPP
