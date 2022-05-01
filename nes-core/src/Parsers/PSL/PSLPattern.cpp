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

#include <API/Query.hpp>
#include <Parsers/PSL/PSLPattern.hpp>
#include <Parsers/PSL/PSLQueryPlanCreator.hpp>
#include <iostream>
namespace NES::Parsers {
PSLPattern::PSLPattern(int id) {
    setId(id);
    setNegated(false);
    setIteration(false);
}
void PSLPattern::printPattern() {
    std::cout << "subPattern of id: " << this->getId() << std::endl;
    std::cout << "right: " << this->getRight() << std::endl;
    std::cout << "left: " << this->getLeft() << std::endl;
    std::cout << "parent: " << this->getParent() << std::endl;
    try {
        std::cout << "eventRight: " << this->getEventRight() << std::endl;

    } catch (std::exception exception) {
        std::cout << "eventRight: null" << std::endl;
    }
    try {
        std::cout << "eventLeft: " << this->getEventLeft() << std::endl;

    } catch (std::exception exception) {
        std::cout << "eventLeft: null" << std::endl;
    }
    try {
        std::cout << "op: " << this->getOp() << std::endl;

    } catch (std::exception exception) {
        std::cout << "op: null" << std::endl;
    }
    std::cout << "negated: " << this->negated << std::endl;
    std::cout << "iteration: " << this->iteration << std::endl;
}

int PSLPattern::getId() const { return id; }
void PSLPattern::setId(int id) { PSLPattern::id = id; }
int PSLPattern::getRight() const { return right; }
void PSLPattern::setRight(int right) { PSLPattern::right = right; }
int PSLPattern::getLeft() const { return left; }
void PSLPattern::setLeft(int left) { PSLPattern::left = left; }
const std::string& PSLPattern::getOp() const { return op; }
void PSLPattern::setOp(const std::string& op) { PSLPattern::op = op; }
int PSLPattern::getParent() const { return parent; }
void PSLPattern::setParent(int parent) { PSLPattern::parent = parent; }
bool PSLPattern::isNegated() const { return negated; }
void PSLPattern::setNegated(bool negated) { PSLPattern::negated = negated; }
bool PSLPattern::isIteration() const { return iteration; }
void PSLPattern::setIteration(bool iteration) { PSLPattern::iteration = iteration; }
const std::string& PSLPattern::getEventRight() const { return eventRight; }
void PSLPattern::setEventRight(const std::string& eventRight) { PSLPattern::eventRight = eventRight; }
const std::string& PSLPattern::getEventLeft() const { return eventLeft; }
void PSLPattern::setEventLeft(const std::string& eventLeft) { PSLPattern::eventLeft = eventLeft; }
uint64_t PSLPattern::getIterMin() const { return iterMin; }
void PSLPattern::setIterMin(uint64_t iterMin) { PSLPattern::iterMin = iterMin; }
uint64_t PSLPattern::getIterMax() const { return iterMax; }
void PSLPattern::setIterMax(uint64_t iterMax) { PSLPattern::iterMax = iterMax; }
const NES::Query& PSLPattern::getQuery() const { return query; }
void PSLPattern::setQuery(const NES::Query& query) { PSLPattern::query = query; }
}// namespace NES::Parsers