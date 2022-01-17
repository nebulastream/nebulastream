
// Generated from /home/zouni/Documents/nebulastream/src/Parsers/NePSL/NesCEP.g4 by ANTLR 4.9.2


#include "NesCEPBaseListener.h"
#include "../../API/NePSLPattern.h"

std::queue< NePSLPattern* > patterns;
int direction; // -1 for right 0 for op 1 for left
NePSLPattern* currentPointer;
NePSLPattern* currentParent;

 void enterEventElem(NesCEPParser::EventElemContext * /*ctx*/)  { }
 void exitEventElem(NesCEPParser::EventElemContext * /*ctx*/)  { }

 void enterOperatorRule(NesCEPParser::OperatorRuleContext * /*ctx*/)  { }
 void exitOperatorRule(NesCEPParser::OperatorRuleContext * /*ctx*/)  { }

 void exitInputStreams(NesCEPParser::InputStreamsContext * /*ctx*/) { }

 void enterInputStream(NesCEPParser::InputStreamContext * /*ctx*/) { }
 void exitInputStream(NesCEPParser::InputStreamContext * /*ctx*/) { }
