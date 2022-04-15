/*
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

#ifndef NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONNODE_HPP_
#define NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONNODE_HPP_

#ifdef MLIR_COMPILER

#include <memory>
#include <vector>

class NESAbstractionNode {
public:
    enum NodeType{ForNode, IfNode, WriteNode, BinOpNode};
    enum BasicType {
        //BasicTypes
        // Type < 5 is INT
        INT1    = 0,
        INT8    = 1,
        INT16   = 2,
        INT32   = 3,
        INT64   = 4,
        // Type < 7 is Float
        FLOAT   = 5,
        DOUBLE  = 6,

        BOOLEAN = 7,
        CHAR    = 8,
        VOID    = 9,

        //DerivedTypes
        ARRAY     = 32,
        CHARARRAY = 33,
        STRUCT    = 34
    };

    NESAbstractionNode(uint64_t nodeId, bool isRootNode, NodeType nodeType);

    virtual ~NESAbstractionNode() = default;

    void setChildNode(std::shared_ptr<NESAbstractionNode> newChildNode);

    std::vector<std::shared_ptr<NESAbstractionNode>> getChildNodes();

    uint64_t getNodeId() const;

    bool getIsRootNode() const;

    NodeType getNodeType() const;


private:
    uint64_t nodeId;
    bool isRootNode;
    const NodeType nodeType;
    std::vector<std::shared_ptr<NESAbstractionNode>> childNodes;
};
#endif //MLIR_COMPILER
#endif //NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONNODE_HPP_

