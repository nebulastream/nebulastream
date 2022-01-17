//
// Created by zouni on 06.01.22.
//

#include <iostream>
#include "NePSLPattern.h"
#include "../../nes-core/src/Parsers/NePSL/NesCEPLexer.h"
#include "../../nes-core//src/Parsers/NePSL/NesCEPParser.h"
#include "../../nes-core/src/Parsers/NePSL/NesCEPListener.h"
#include "../../nes-core/src/Parsers/NePSL/NesCEPBaseListener.h"

void processPattern(const std::string* patternString){

    std::cout<<patternString;
    antlr4::ANTLRInputStream input((std::istream&) patternString);
    NesCEPLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    NesCEPParser parser(&tokens);
    NesCEPParser::QueryContext* tree=parser.query();
    std::cout << tree->toStringTree(&parser) << std::endl;

    NesCEPBaseListener listener;
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(&listener, tree);

}

NePSLPattern::NePSLPattern(int id) {
    setId(id);
    setNegated(false);
    setIteration(false);
    setParent(this);
}

int NePSLPattern::getId() const { return id; }
void NePSLPattern::setId(int id) { NePSLPattern::id = id; }
NePSLPattern* NePSLPattern::getRight() const { return right; }
void NePSLPattern::setRight(NePSLPattern* right) { NePSLPattern::right = right; }
NePSLPattern* NePSLPattern::getLeft() const { return left; }
void NePSLPattern::setLeft(NePSLPattern* left) { NePSLPattern::left = left; }
NePSLPattern* NePSLPattern::getOp() const { return op; }
void NePSLPattern::setOp(NePSLPattern* op) { NePSLPattern::op = op; }
NePSLPattern* NePSLPattern::getParent() const { return parent; }
void NePSLPattern::setParent(NePSLPattern* parent) { NePSLPattern::parent = parent; }
bool NePSLPattern::isNegated() const { return negated; }
void NePSLPattern::setNegated(bool negated) { NePSLPattern::negated = negated; }
bool NePSLPattern::isIteration() const { return iteration; }
void NePSLPattern::setIteration(bool iteration) { NePSLPattern::iteration = iteration; }
const std::string& NePSLPattern::getIterStr() const { return iterStr; }
void NePSLPattern::setIterStr(const std::string& iterStr) { NePSLPattern::iterStr = iterStr; }
void NePSLPattern::processPattern(const std::string patternString) {
    std::cout << (patternString);
}
