
// Generated from /worktrees/topology-editor/nes-sql-parser/AntlrSQL.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)




class  AntlrSQLLexer : public antlr4::Lexer {
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
    RECOVER = 58, RIGHT = 59, RLIKE = 60, ROLLUP = 61, SCHEMA = 62, SELECT = 63, 
    SETS = 64, SOME = 65, START = 66, TABLE = 67, TO = 68, TRUE = 69, TYPE = 70, 
    UNION = 71, UNKNOWN = 72, USE = 73, USING = 74, VALUES = 75, WHEN = 76, 
    WHERE = 77, WINDOW = 78, WITH = 79, SET = 80, TUMBLING = 81, SLIDING = 82, 
    THRESHOLD = 83, SIZE = 84, ADVANCE = 85, MS = 86, SEC = 87, MINUTE = 88, 
    HOUR = 89, DAY = 90, MIN = 91, MAX = 92, AVG = 93, SUM = 94, COUNT = 95, 
    MEDIAN = 96, WATERMARK = 97, OFFSET = 98, CSV_FORMAT = 99, AT_MOST_ONCE = 100, 
    AT_LEAST_ONCE = 101, JSON = 102, TEXT = 103, EXPLAIN = 104, MODEL = 105, 
    MODELS = 106, MODEL_INFERENCE = 107, INPUT = 108, OUTPUT = 109, BOOLEAN_VALUE = 110, 
    EQ = 111, NSEQ = 112, NEQ = 113, NEQJ = 114, LT = 115, LTE = 116, GT = 117, 
    GTE = 118, PLUS = 119, MINUS = 120, ASTERISK = 121, SLASH = 122, PERCENT = 123, 
    TILDE = 124, AMPERSAND = 125, PIPE = 126, CONCAT_PIPE = 127, HAT = 128, 
    STRING = 129, INTEGER_VALUE = 130, FLOAT_LITERAL = 131, WS = 132, SINKS = 133, 
    SOURCES = 134, QUERIES = 135, DATA_TYPE = 136, INTEGER_UNSIGNED_TYPE = 137, 
    INTEGER_SIGNED_TYPE = 138, INTEGER_BASES_TYPES = 139, TINY_INT_TYPE = 140, 
    SMALL_INT_TYPE = 141, NORMAL_INT_TYPE = 142, BIG_INT_TYPE = 143, FLOATING_POINT_TYPE = 144, 
    CHAR_TYPE = 145, VARSIZED_TYPE = 146, BOOLEAN_TYPE = 147, UNSIGNED_TYPE_QUALIFIER = 148, 
    SHOW = 149, FORMAT = 150, CREATE = 151, SOURCE = 152, LOGICAL = 153, 
    PHYSICAL = 154, WORKER = 155, SINK = 156, SIMPLE_COMMENT = 157, BRACKETED_COMMENT = 158, 
    IDENTIFIER = 159, UNRECOGNIZED = 160
  };

  explicit AntlrSQLLexer(antlr4::CharStream *input);

  ~AntlrSQLLexer() override;


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
  bool FLOAT_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool BRACKETED_COMMENTSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);

};

