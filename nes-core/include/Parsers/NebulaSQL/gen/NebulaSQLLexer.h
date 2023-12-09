
// Generated from NebulaSQL.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"


namespace NES::Parsers {


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
    OFFSET = 95, ZMQ = 96, KAFKA = 97, FILE = 98, MQTT = 99, OPC = 100, 
    PRINT = 101, LOCALHOST = 102, CSV_FORMAT = 103, NES_FORMAT = 104, TEXT_FORMAT = 105, 
    AT_MOST_ONCE = 106, AT_LEAST_ONCE = 107, BOOLEAN_VALUE = 108, EQ = 109, 
    NSEQ = 110, NEQ = 111, NEQJ = 112, LT = 113, LTE = 114, GT = 115, GTE = 116, 
    PLUS = 117, MINUS = 118, ASTERISK = 119, SLASH = 120, PERCENT = 121, 
    TILDE = 122, AMPERSAND = 123, PIPE = 124, CONCAT_PIPE = 125, HAT = 126, 
    STRING = 127, BIGINT_LITERAL = 128, SMALLINT_LITERAL = 129, TINYINT_LITERAL = 130, 
    INTEGER_VALUE = 131, EXPONENT_VALUE = 132, DECIMAL_VALUE = 133, FLOAT_LITERAL = 134, 
    DOUBLE_LITERAL = 135, BIGDECIMAL_LITERAL = 136, IDENTIFIER = 137, SIMPLE_COMMENT = 138, 
    BRACKETED_COMMENT = 139, WS = 140, FOUR_OCTETS = 141, OCTET = 142, UNRECOGNIZED = 143
  };

  explicit NebulaSQLLexer(antlr4::CharStream *input);
  ~NebulaSQLLexer();


    /**
     * Verify whether current token is a valid decimal token (which contains dot).
     * Returns true if the character that follows the token is not a digit or letter or underscore.
     *
     * For example:
     * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
     * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
     * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
     * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
     * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
     * which is not a digit or letter or underscore.
     */
    bool isValidDecimal() {
      int nextChar = _input->LA(1);
      if ((nextChar >= 'A' && nextChar <= 'Z') || (nextChar >= '0' && nextChar <= '9') ||
        nextChar == '_') {
        return false;
      } else {
        return true;
      }
    }

    /**
     * This method will be called when we see '*' and try to match it as a bracketed comment.
     * If the next character is '+', it should be parsed as hint later, and we cannot match
     * it as a bracketed comment.
     *
     * Returns true if the next character is '+'.
     */
    bool isHint() {
      int nextChar = _input->LA(1);
      if (nextChar == '+') {
        return true;
      } else {
        return false;
      }
    }

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.
  bool EXPONENT_VALUESempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool DECIMAL_VALUESempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool FLOAT_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool DOUBLE_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool BIGDECIMAL_LITERALSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);
  bool BRACKETED_COMMENTSempred(antlr4::RuleContext *_localctx, size_t predicateIndex);

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace NES::Parsers
