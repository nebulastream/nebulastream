
// Generated from CLionProjects/nebulastream/nes-coordinator/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_
#pragma once

#include <antlr4-runtime/antlr4-runtime.h>

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
        T__8 = 9,
        T__9 = 10,
        T__10 = 11,
        T__11 = 12,
        T__12 = 13,
        T__13 = 14,
        T__14 = 15,
        T__15 = 16,
        T__16 = 17,
        T__17 = 18,
        T__18 = 19,
        T__19 = 20,
        T__20 = 21,
        INT = 22,
        FLOAT = 23,
        PROTOCOL = 24,
        FILETYPE = 25,
        PORT = 26,
        WS = 27,
        FROM = 28,
        PATTERN = 29,
        WHERE = 30,
        WITHIN = 31,
        CONSUMING = 32,
        SELECT = 33,
        INTO = 34,
        ALL = 35,
        ANY = 36,
        SEP = 37,
        COMMA = 38,
        LPARENTHESIS = 39,
        RPARENTHESIS = 40,
        NOT = 41,
        NOT_OP = 42,
        SEQ = 43,
        NEXT = 44,
        AND = 45,
        OR = 46,
        STAR = 47,
        PLUS = 48,
        D_POINTS = 49,
        LBRACKET = 50,
        RBRACKET = 51,
        XOR = 52,
        IN = 53,
        IS = 54,
        NULLTOKEN = 55,
        BETWEEN = 56,
        BINARY = 57,
        TRUE = 58,
        FALSE = 59,
        UNKNOWN = 60,
        QUARTER = 61,
        MONTH = 62,
        DAY = 63,
        HOUR = 64,
        MINUTE = 65,
        WEEK = 66,
        SECOND = 67,
        MICROSECOND = 68,
        AS = 69,
        EQUAL = 70,
        KAFKA = 71,
        FILE = 72,
        MQTT = 73,
        NETWORK = 74,
        NULLOUTPUT = 75,
        OPC = 76,
        PRINT = 77,
        ZMQ = 78,
        POINT = 79,
        QUOTE = 80,
        AVG = 81,
        SUM = 82,
        MIN = 83,
        MAX = 84,
        COUNT = 85,
        IF = 86,
        LOGOR = 87,
        LOGAND = 88,
        LOGXOR = 89,
        NONE = 90,
        URL = 91,
        NAME = 92,
        ID = 93,
        PATH = 94
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
#endif// NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLEXER_H_
