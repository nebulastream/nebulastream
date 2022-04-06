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

#include <iostream>
#include <API/NePSLPattern.h>
#include <Parsers/NePSL/gen/NesCEPLexer.h>
#include <Parsers/NePSL/gen/NesCEPParser.h>
#include <Parsers/NePSL/gen/NesCEPListener.h>
#include <Parsers/NePSL/gen/NesCEPBaseListener.h>
#include <Parsers/NePSL/NesCEPQueryPlanCreator.h>
#include <API/Query.hpp>
#include <cstring>

NePSLPattern::NePSLPattern(int id) {
    setId(id);
    setNegated(false);
    setIteration(false);

}
void NePSLPattern::printPattern() {
    std::cout<<"subPattern of id: "<<this->getId()<<std::endl;
    std::cout<<"right: "<<this->getRight()<<std::endl;
    std::cout<<"left: "<<this->getLeft()<<std::endl;
    std::cout<<"parent: "<<this->getParent()<<std::endl;
    try {
        std::cout<<"eventRight: "<<this->getEventRight()<<std::endl;

    } catch (std::exception exception) {
        std::cout<<"eventRight: null"<<std::endl;
    }
    try {
        std::cout<<"eventLeft: "<<this->getEventLeft()<<std::endl;

    } catch (std::exception exception) {
        std::cout<<"eventLeft: null"<<std::endl;
    }
    try {
        std::cout<<"op: "<<this->getOp()<<std::endl;

    } catch (std::exception exception) {
        std::cout<<"op: null"<<std::endl;
    }
    std::cout<<"negated: "<<this->negated<<std::endl;
    std::cout<<"iteration: "<<this->iteration<<std::endl;
}


int NePSLPattern::getId() const { return id; }
void NePSLPattern::setId(int id) { NePSLPattern::id = id; }
int NePSLPattern::getRight() const { return right; }
void NePSLPattern::setRight(int right) { NePSLPattern::right = right; }
int NePSLPattern::getLeft() const { return left; }
void NePSLPattern::setLeft(int left) { NePSLPattern::left = left; }
const std::string& NePSLPattern::getOp() const { return op; }
void NePSLPattern::setOp(const std::string& op) { NePSLPattern::op = op; }
int NePSLPattern::getParent() const { return parent; }
void NePSLPattern::setParent(int parent) { NePSLPattern::parent = parent; }
bool NePSLPattern::isNegated() const { return negated; }
void NePSLPattern::setNegated(bool negated) { NePSLPattern::negated = negated; }
bool NePSLPattern::isIteration() const { return iteration; }
void NePSLPattern::setIteration(bool iteration) { NePSLPattern::iteration = iteration; }
const std::string& NePSLPattern::getEventRight() const { return eventRight; }
void NePSLPattern::setEventRight(const std::string& eventRight) { NePSLPattern::eventRight = eventRight; }
const std::string& NePSLPattern::getEventLeft() const { return eventLeft; }
void NePSLPattern::setEventLeft(const std::string& eventLeft) { NePSLPattern::eventLeft = eventLeft; }
uint64_t NePSLPattern::getIterMin() const { return iterMin; }
void NePSLPattern::setIterMin(uint64_t iterMin) { NePSLPattern::iterMin = iterMin; }
uint64_t NePSLPattern::getIterMax() const { return iterMax; }
void NePSLPattern::setIterMax(uint64_t iterMax) { NePSLPattern::iterMax = iterMax; }
const NES::Query& NePSLPattern::getQuery() const { return query; }
void NePSLPattern::setQuery(const NES::Query& query) { NePSLPattern::query = query; }