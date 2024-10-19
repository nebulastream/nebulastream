
// Generated from /home/rudi/dima/nebulastream-public/nes-nebuli/parser/NebulaSQL.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)




class  NebulaSQLLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    BACKQUOTED_IDENTIFIER = 8, ALL = 9, AND = 10, ANY = 11, AS = 12, ASC = 13, 
    AT = 14, BETWEEN = 15, BY = 16, COMMENT = 17, CUBE = 18, DELETE = 19, 
    DESC = 20, DISTINCT = 21, DIV = 22, DROP = 23, ELSE = 24, END = 25, 
    ESCAPE = 26, EXISTS = 27, FALSE = 28, FIRST = 29, FOR = 30, FROM = 31, 
    FULL = 32, GROUP = 33, GROUPING = 34, HAVING = 35, IF = 36, IN = 37, 
    INNER = 38, INSERT = 39, INTO = 40, IS = 41, JOIN = 42, LAST = 43, LEFT = 44, 
    LIKE = 45, LIMIT = 46, LIST = 47, MERGE = 48, NATURAL = 49, NOT = 50, 
    NULLTOKEN = 51, NULLS = 52, OF = 53, ON = 54, OR = 55, ORDER = 56, QUERY = 57, 
    RECOVER = 58, RIGHT = 59, RLIKE = 60, ROLLUP = 61, SELECT = 62, SETS = 63, 
    SOME = 64, START = 65, TABLE = 66, TO = 67, TRUE = 68, TYPE = 69, UNION = 70, 
    UNKNOWN = 71, USE = 72, USING = 73, VALUES = 74, WHEN = 75, WHERE = 76, 
    WINDOW = 77, WITH = 78, TUMBLING = 79, SLIDING = 80, THRESHOLD = 81, 
    SIZE = 82, ADVANCE = 83, MS = 84, SEC = 85, MIN = 86, HOUR = 87, DAY = 88, 
    MAX = 89, AVG = 90, SUM = 91, COUNT = 92, MEDIAN = 93, WATERMARK = 94, 
    OFFSET = 95, FILE = 96, PRINT = 97, LOCALHOST = 98, CSV_FORMAT = 99, 
    AT_MOST_ONCE = 100, AT_LEAST_ONCE = 101, BOOLEAN_VALUE = 102, EQ = 103, 
    NSEQ = 104, NEQ = 105, NEQJ = 106, LT = 107, LTE = 108, GT = 109, GTE = 110, 
    PLUS = 111, MINUS = 112, ASTERISK = 113, SLASH = 114, PERCENT = 115, 
    TILDE = 116, AMPERSAND = 117, PIPE = 118, CONCAT_PIPE = 119, HAT = 120, 
    STRING = 121, BIGINT_LITERAL = 122, SMALLINT_LITERAL = 123, TINYINT_LITERAL = 124, 
    INTEGER_VALUE = 125, EXPONENT_VALUE = 126, DECIMAL_VALUE = 127, FLOAT_LITERAL = 128, 
    DOUBLE_LITERAL = 129, BIGDECIMAL_LITERAL = 130, IDENTIFIER = 131, SIMPLE_COMMENT = 132, 
    BRACKETED_COMMENT = 133, WS = 134, FOUR_OCTETS = 135, OCTET = 136, UNRECOGNIZED = 137
  };

  explicit NebulaSQLLexer(antlr4::CharStream *input);

  ~NebulaSQLLexer() override;


    bool isValidDecimal() {
      int nextChar = _input->LA(1);
      if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
        nextChar == '_') {
        return false;
      } else {
        return true;
      }
    }

    bool isHint() {
      int nextChar = _input->LA(1);
      if (nextChar == '+') {
        return true;
      } else {
        return false;
      }
    }


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.
  bool EXPONENT_VALUESempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool DECIMAL_VALUESempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool FLOAT_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool DOUBLE_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool BIGDECIMAL_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool BRACKETED_COMMENTSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);

};

