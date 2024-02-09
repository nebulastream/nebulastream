
// Generated from IoTDB/nes-core/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_

#include <antlr4-runtime.h>

namespace NES::Parsers {

class NesCEPLexer : public antlr4::Lexer {
  public:
    enum {
        T__0 = 1,
        T__1 = 2,
        T__2 = 3,
        T__3 = 4,
        T__4 = 5,
        T__5 = 6,
        T__6 = 7,
        T__7 = 8,
        WS = 9,
        FROM = 10,
        PATTERN = 11,
        WHERE = 12,
        WITHIN = 13,
        CONSUMING = 14,
        RETURN = 15,
        INTO = 16,
        ALL = 17,
        ANY = 18,
        SEP = 19,
        COMMA = 20,
        LPARENTHESIS = 21,
        RPARENTHESIS = 22,
        NOT = 23,
        NOT_OP = 24,
        SEQ = 25,
        NEXT = 26,
        AND = 27,
        OR = 28,
        STAR = 29,
        PLUS = 30,
        D_POINTS = 31,
        LBRACKET = 32,
        RBRACKET = 33,
        XOR = 34,
        IN = 35,
        IS = 36,
        NULLTOKEN = 37,
        BETWEEN = 38,
        BINARY = 39,
        TRUE = 40,
        FALSE = 41,
        UNKNOWN = 42,
        QUARTER = 43,
        MONTH = 44,
        DAY = 45,
        HOUR = 46,
        MINUTE = 47,
        WEEK = 48,
        SECOND = 49,
        MICROSECOND = 50,
        AS = 51,
        EQUAL = 52,
        SINKSEP = 53,
        KAFKA = 54,
        FILE = 55,
        MQTT = 56,
        NETWORK = 57,
        NULLOUTPUT = 58,
        OPC = 59,
        PRINT = 60,
        ZMQ = 61,
        POINT = 62,
        QUOTE = 63,
        AVG = 64,
        SUM = 65,
        MIN = 66,
        MAX = 67,
        COUNT = 68,
        IF = 69,
        LOGOR = 70,
        LOGAND = 71,
        LOGXOR = 72,
        NONE = 73,
        INT = 74,
        NAME = 75,
        ID = 76
    };

    explicit NesCEPLexer(antlr4::CharStream* input);
    ~NesCEPLexer();

    virtual std::string getGrammarFileName() const override;
    virtual const std::vector<std::string>& getRuleNames() const override;

    virtual const std::vector<std::string>& getChannelNames() const override;
    virtual const std::vector<std::string>& getModeNames() const override;
    virtual const std::vector<std::string>& getTokenNames() const override;// deprecated, use vocabulary instead
    virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

    virtual const std::vector<uint16_t> getSerializedATN() const override;
    virtual const antlr4::atn::ATN& getATN() const override;

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

    struct Initializer {
        Initializer();
    };
    static Initializer _init;
};

}// namespace NES::Parsers
#endif // NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_
