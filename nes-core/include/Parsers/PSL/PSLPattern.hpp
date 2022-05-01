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

#ifndef NES_NEPSLPATTERN_H
#define NES_NEPSLPATTERN_H

#include <API/Query.hpp>
#include <list>
#include <queue>
#include <string>

namespace NES::Parsers {

class PSLPattern {
  private:
    int id;
    int right = -1;
    int left = -1;
    std::string op;
    int parent = -1;
    bool negated;
    bool iteration;
    uint64_t iterMin;
    uint64_t iterMax;
    std::string eventRight;
    std::string eventLeft;
    NES::Query query = NES::Query(NULL);

  public:
    explicit PSLPattern(int id);
    PSLPattern* const* getPatternList() const;
    int getId() const;
    void setId(int id);
    int getRight() const;
    void setRight(int right);
    int getLeft() const;
    void setLeft(int left);
    const std::string& getOp() const;
    void setOp(const std::string& op);
    int getParent() const;
    void setParent(int parent);
    bool isNegated() const;
    void setNegated(bool negated);
    bool isIteration() const;
    void setIteration(bool iteration);
    const std::string& getEventRight() const;
    void setEventRight(const std::string& eventRight);
    const std::string& getEventLeft() const;
    void setEventLeft(const std::string& eventLeft);
    uint64_t getIterMin() const;
    void setIterMin(uint64_t iterMin);
    uint64_t getIterMax() const;
    void setIterMax(uint64_t iterMax);
    const NES::Query& getQuery() const;
    void setQuery(const NES::Query& query);
    void printPattern();
};
}// namespace NES::Parsers
#endif//NES_NEPSLPATTERN_H
