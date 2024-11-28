
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
        INT = 17,
        FLOAT = 18,
        PROTOCOL = 19,
        FILETYPE = 20,
        PORT = 21,
        WS = 22,
        FROM = 23,
        PATTERN = 24,
        WHERE = 25,
        WITHIN = 26,
        CONSUMING = 27,
        SELECT = 28,
        INTO = 29,
        ALL = 30,
        ANY = 31,
        SEP = 32,
        COMMA = 33,
        LPARENTHESIS = 34,
        RPARENTHESIS = 35,
        NOT = 36,
        NOT_OP = 37,
        SEQ = 38,
        NEXT = 39,
        AND = 40,
        OR = 41,
        STAR = 42,
        PLUS = 43,
        D_POINTS = 44,
        LBRACKET = 45,
        RBRACKET = 46,
        XOR = 47,
        IN = 48,
        IS = 49,
        NULLTOKEN = 50,
        BETWEEN = 51,
        BINARY = 52,
        TRUE = 53,
        FALSE = 54,
        UNKNOWN = 55,
        QUARTER = 56,
        MONTH = 57,
        DAY = 58,
        HOUR = 59,
        MINUTE = 60,
        WEEK = 61,
        SECOND = 62,
        MICROSECOND = 63,
        AS = 64,
        EQUAL = 65,
        KAFKA = 66,
        FILE = 67,
        MQTT = 68,
        NETWORK = 69,
        NULLOUTPUT = 70,
        OPC = 71,
        PRINT = 72,
        ZMQ = 73,
        POINT = 74,
        QUOTE = 75,
        AVG = 76,
        SUM = 77,
        MIN = 78,
        MAX = 79,
        COUNT = 80,
        IF = 81,
        LOGOR = 82,
        LOGAND = 83,
        LOGXOR = 84,
        NONE = 85,
        URL = 86,
        NAME = 87,
        ID = 88,
        PATH = 89
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
