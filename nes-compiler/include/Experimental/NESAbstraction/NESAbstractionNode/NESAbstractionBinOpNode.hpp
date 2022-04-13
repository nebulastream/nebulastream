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

#ifndef MLIR_APPROACH_NESABSTRACTIONBINOPNODE_HPP
#define MLIR_APPROACH_NESABSTRACTIONBINOPNODE_HPP

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionIfNode.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <cstdint>
#include <vector>

class NESAbstractionBinOpNode : public NESAbstractionNode {
public:
    class Operand {
        public: 
            enum OperandType{Constant, FieldAccess};

            Operand(OperandType operandType) : operandType(operandType){};
            virtual ~Operand() = default;

            OperandType getOperandType() const { return operandType; }
            virtual int64_t getValue() = 0; //Todo should be generic and not fixed int64_t
            virtual NESAbstractionNode::BasicType getValueType() = 0;

        private:
            OperandType operandType;
    };
    class FieldAccessOperand : public Operand {
        public:
            FieldAccessOperand(int64_t index, NESAbstractionNode::BasicType valueType) : 
                                Operand(FieldAccess), index(index), valueType(valueType) {};
            ~FieldAccessOperand() override = default;

            int64_t getValue() override { //Todo should be generic and not fixed int64_t
                return index;
            }
            NESAbstractionNode::BasicType getValueType() override {
                return valueType;
            }

            static bool classof(const Operand *operand) {
                return operand->getOperandType() == FieldAccess;
            }
        private:
            int64_t index;
            NESAbstractionNode::BasicType valueType;
    };
    class ConstantOperand : public Operand {
        public:
            ConstantOperand(int64_t value, NESAbstractionNode::BasicType valueType) : 
                            Operand(Constant), value(value), valueType(valueType) {};
            ~ConstantOperand() override = default;

            int64_t getValue() override { //Todo should be generic and not fixed int64_t
                return value;
            }
            NESAbstractionNode::BasicType getValueType() override {
                return valueType;
            }

            static bool classof(const Operand *operand) {
                return operand->getOperandType() == Constant;
            }
        private:
            int64_t value; //Todo should be generic and not fixed int64_t
            NESAbstractionNode::BasicType valueType;
    };

    enum Operation{sub, add, fsub, fadd, min, max, umax, umin, _and, _or, _xor};

    NESAbstractionBinOpNode(uint64_t nodeId, bool isRootNode, std::unique_ptr<Operand> leftVal, 
                            std::unique_ptr<Operand> rightVal, Operation operation) : 
                            NESAbstractionNode(nodeId, isRootNode, NodeType::BinOpNode),
                            leftVal(std::move(leftVal)), rightVal(std::move(rightVal)), operation(operation) {};
    ~NESAbstractionBinOpNode() override = default;

    Operation getOperation() { return operation; }
    
    int64_t getLeftVal() { return leftVal->getValue(); } //Todo should be generic and not fixed int64_t
    
    int64_t getRightVal() { return rightVal->getValue(); } //Todo should be generic and not fixed int64_t

    std::vector<Operand::OperandType> getOperandTypes() { return {leftVal->getOperandType(), rightVal->getOperandType()}; }

    std::vector<NESAbstractionNode::BasicType> getValueTypes() { return {leftVal->getValueType(), rightVal->getValueType()}; }

    static bool classof(const NESAbstractionNode *Node) { 
        return Node->getNodeType() == NESAbstractionNode::NodeType::BinOpNode;
    };

private:
    std::unique_ptr<Operand> leftVal;
    std::unique_ptr<Operand> rightVal;
    Operation operation;
    std::vector<std::shared_ptr<NESAbstractionNode>> childNodes;
};
#endif //MLIR_APPROACH_NESABSTRACTIONBINOPNODE_HPP
