#include <emscripten/bind.h>
#include <antlr4-runtime.h>
#include "AntlrSQLLexer.h"
#include "AntlrSQLParser.h"
#include <string>

std::string parseSql(const std::string& sql) {
    try {
        antlr4::ANTLRInputStream input(sql);
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);

        // Suppress ANTLR4 error output to stderr
        parser.removeErrorListeners();
        lexer.removeErrorListeners();

        auto* tree = parser.singleStatement();
        if (parser.getNumberOfSyntaxErrors() > 0) {
            return "PARSE_ERROR";
        }
        return tree->toStringTree(&parser);
    } catch (const std::exception& e) {
        return "PARSE_ERROR";
    } catch (...) {
        return "PARSE_ERROR";
    }
}

EMSCRIPTEN_BINDINGS(spike_antlr4) {
    emscripten::function("parseSql", &parseSql);
}
