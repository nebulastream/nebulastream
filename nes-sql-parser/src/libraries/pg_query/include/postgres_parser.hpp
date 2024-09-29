//===----------------------------------------------------------------------===//
//                         
//
// postgres_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include "nodes/pg_list.hpp"
#include "pg_simplified_token.hpp"

namespace pg_parser {
class PostgresParser {
public:
	PostgresParser();
	~PostgresParser();

	bool success;
	pgquery::PGList *parse_tree;
	std::string error_message;
	int error_location;
public:
	void Parse(const std::string &query);
	static std::vector<pgquery::PGSimplifiedToken> Tokenize(const std::string &query);

	static bool IsKeyword(const std::string &text);
	static std::vector<pgquery::PGKeyword> KeywordList();

	static void SetPreserveIdentifierCase(bool downcase);
};

}
