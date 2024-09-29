#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"
#include "parser/scansup.hpp"
#include "common/keywords.hpp"

namespace pg_parser {

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {}

void PostgresParser::Parse(const std::string &query) {
	pgquery::pg_parser_init();
	pgquery::parse_result res;
	pg_parser_parse(query.c_str(), &res);
	success = res.success;

	if (success) {
		parse_tree = res.parse_tree;
	} else {
		error_message = std::string(res.error_message);
		error_location = res.error_location;
	}
}

std::vector<pgquery::PGSimplifiedToken> PostgresParser::Tokenize(const std::string &query) {
	pgquery::pg_parser_init();
	auto tokens = pgquery::tokenize(query.c_str());
	pgquery::pg_parser_cleanup();
	return std::move(tokens);
}

PostgresParser::~PostgresParser()  {
    pgquery::pg_parser_cleanup();
}

bool PostgresParser::IsKeyword(const std::string &text) {
	return pgquery::is_keyword(text.c_str());
}

std::vector<pgquery::PGKeyword> PostgresParser::KeywordList() {
	std::vector<pgquery::PGKeyword> tmp(pgquery::keyword_list());
	return tmp;
}

void PostgresParser::SetPreserveIdentifierCase(bool preserve) {
	pgquery::set_preserve_identifier_case(preserve);
}

}
