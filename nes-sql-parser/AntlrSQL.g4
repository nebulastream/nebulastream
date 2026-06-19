/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

grammar AntlrSQL;

@lexer::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::members {
      bool SQL_standard_keyword_behavior = false;
      bool legacy_exponent_literal_as_decimal_enabled = false;
}

@lexer::members {
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
}

singleStatement: statement SEMICOLON? EOF;
SEMICOLON: ';';

terminatedStatement: statement SEMICOLON;
multipleStatements: (statement (SEMICOLON statement)* SEMICOLON?)? EOF;
statement: queryWithOptions | createStatement | dropStatement | showStatement | explainStatement;

explainStatement: EXPLAIN query;
EXPLAIN: E X P L A I N;
createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition | createWorkerDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                optionsClause?;
FOR: F O R;
TYPE: T Y P E;
optionsClause: (SET OPEN_PARANTHESIS options=namedConfigLiteralSeq? CLOSE_PARANTHESIS);
SET: S E T;

namedConfigLiteralSeq: namedConfigLiteral (COMMA namedConfigLiteral)*;
namedConfigLiteral: literal AS qualifiedIdentifier;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier optionsClause?;

createWorkerDefinition: WORKER hostaddr=STRING_LITERAL optionsClause?;

schemaDefinition: OPEN_PARANTHESIS columnDefinition (COMMA columnDefinition)* CLOSE_PARANTHESIS;
columnDefinition: identifier typeDefinition nullableDefinition?;

typeDefinition: DATA_TYPE;
nullableDefinition: NOT NULL;
NULL: N U L L;

dropStatement: DROP dropSubject WHERE dropFilter;
DROP: D R O P;
WHERE: W H E R E;
dropSubject: dropQuery | dropSource | dropSink | dropWorker;
dropQuery: QUERY;
QUERY: Q U E R Y;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE;
dropPhysicalSourceSubject: PHYSICAL SOURCE;
dropWorker: WORKER;
dropSink: SINK;

dropFilter: attr=identifier EQ value=literal;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT identifier)?;
showSubject: showQueriesSubject | showLogicalSourcesSubject | showPhysicalSourcesSubject | showSinksSubject;
showQueriesSubject: QUERIES;
showLogicalSourcesSubject: LOGICAL SOURCES;
showPhysicalSourcesSubject: PHYSICAL SOURCES;
showSinksSubject: SINKS;

showFilter: attr=identifier EQ value=literal;

queryWithOptions: queryWithSink optionsClause?;
queryWithSink: unionSeq intoTerm;
unionSeq: query (UNION_ALL query)*;
query: projectionTerm fromTerm filterTerm? groupByTerm?;

projectionTerm: SELECT projectionSeq?;
SELECT: S E L E C T;
projectionSeq: projection (COMMA projection)*;
projection: ASTERISK | namedExpression;
ASTERISK: '*';
namedExpression: openScalarExpression (AS identifier)?;

filterTerm: WHERE openScalarExpression;
fromTerm: FROM openRelationalExpression;
FROM: F R O M;
groupByTerm: GROUP_BY openScalarExpression (COMMA openScalarExpression)* unaryWindowLiteral;
GROUP_BY: GROUP WS BY;
fragment GROUP: G R O U P;
fragment BY: B Y;

intoTerm: INTO sink;
INTO: I N T O;
sink: identifier | literalFunctionCall;

openRelationalExpression: closedRelationalExpression | joinRelationalExpression;
closedRelationalExpression: identifier | functionCall | OPEN_PARANTHESIS openRelationalExpression CLOSE_PARANTHESIS | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS | closedRelationalExpression AS identifier;
AS: A S;
joinRelationalExpression: closedRelationalExpression INNER_JOIN closedRelationalExpression ON openScalarExpression binaryWindowLiteral;

binaryWindowLiteral: WINDOW WINDOW_TYPE OPEN_PARANTHESIS IDENTIFIER COMMA IDENTIFIER COMMA intervalLiteral CLOSE_PARANTHESIS;
unaryWindowLiteral: WINDOW WINDOW_TYPE OPEN_PARANTHESIS IDENTIFIER COMMA intervalLiteral CLOSE_PARANTHESIS;
WINDOW_TYPE: TUMBLING | SLIDING;
fragment TUMBLING: T U M B L I N G;
fragment SLIDING: S L I D I N G;
WINDOW: W I N D O W;
ON: O N;
INNER_JOIN: INNER? JOIN;
fragment INNER: I N N E R WS;
fragment JOIN: J O I N;

UNION_ALL: UNION ALL?;
fragment ALL: A L L;
fragment UNION: U N I O N;

openScalarExpression: infixScalarExpression | intervalExpression | closedScalarExpression;
closedScalarExpression: identifier
    | literal
    | prefixScalarExpression
    | castFunctionalScalarExpression
    | functionCall
    | OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS
    | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS;

prefixScalarExpression: prefixScalarOperator closedScalarExpression;
infixScalarExpression: closedScalarExpression infixScalarOperator closedScalarExpression;
castFunctionalScalarExpression: DATA_TYPE OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS;

functionCall: positionalFunctionCall | namedFunctionCall;
positionalFunctionCall: identifier OPEN_PARANTHESIS positionalFunctionCallArgument (COMMA positionalFunctionCallArgument)* CLOSE_PARANTHESIS;
positionalFunctionCallArgument: closedScalarExpression | closedRelationalExpression | schemaDefinition;
namedFunctionCall: identifier OPEN_PARANTHESIS namedFunctionCallArgument (COMMA namedFunctionCallArgument)* CLOSE_PARANTHESIS;
namedFunctionCallArgument: positionalFunctionCallArgument AS qualifiedIdentifier;

/// Once we have better literal folding, this separate block can be removed and its usages be replaced by functionCall
literalFunctionCall: positionalLiteralFunctionCall | namedLiteralFunctionCall;
positionalLiteralFunctionCall: identifier OPEN_PARANTHESIS positionalLiteralFunctionCallArgument (COMMA positionalLiteralFunctionCallArgument)* CLOSE_PARANTHESIS;
positionalLiteralFunctionCallArgument: literal | schemaDefinition;
namedLiteralFunctionCall: identifier OPEN_PARANTHESIS namedLiteralFunctionCallArgument (COMMA namedLiteralFunctionCallArgument)* CLOSE_PARANTHESIS;
namedLiteralFunctionCallArgument: positionalLiteralFunctionCallArgument AS qualifiedIdentifier;

OPEN_PARANTHESIS: '(';
CLOSE_PARANTHESIS: ')';
COMMA: ',';

intervalLiteral: INTERVAL LITERAL DATETIME_FIELD;
intervalExpression: INTERVAL openScalarExpression DATETIME_FIELD;
DATETIME_FIELD: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECONDS;
fragment YEAR: Y E A R;
fragment MONTH: M O N T H;
fragment DAY: D A Y;
fragment HOUR: H O U R;
fragment MINUTE: M I N U T E;
fragment SECOND: S E C O N D;
fragment MILLISECONDS: M I L L I S E C O N D S;

INTERVAL: I N T E R V A L;

prefixScalarOperator: MINUS | NOT;
infixScalarOperator: MINUS | PLUS | EQ | DIV | NEQ | AND | OR | GREATER | GREATER_EQUALS | LESSER | LESSER_EQUALS;
MINUS: '-';
PLUS: '+';
DIV: '/';
MUL: ASTERISK;
NOT: N O T;
EQ: '=';
NEQ: '<>' | '!=';
AND: A N D;
OR: O R;
GREATER: '>';
GREATER_EQUALS: '>=';
LESSER: '<';
LESSER_EQUALS: '<=';


literal: LITERAL;
LITERAL: BOOLEAN_TYPE | STRING_LITERAL | SIGNED_INT_LITERAL | UNSIGNED_INT_LITERAL | FLOAT_LITERAL;

BOOL_LITERAL: TRUE | FALSE;
fragment TRUE: T R U E;
fragment FALSE: F A L S E;


STRING_LITERAL: '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' ;

SIGNED_INT_LITERAL: MINUS? DIGITS;
UNSIGNED_INT_LITERAL: DIGITS;

FLOAT_LITERAL: EXP_FRACTION | DECIMAL_FRACTION;
fragment EXP_FRACTION: (MINUS? E DIGITS)? MINUS? DIGITS;
fragment DECIMAL_FRACTION : MINUS? DIGITS DOT DIGIT* | MINUS? DOT DIGITS;

fragment DIGITS: DIGIT+;
fragment DIGIT : [0-9] ;

fragment LETTER : ('a'..'z'|'A'..'Z'|'_') ;

SINKS: S I N K S;
SOURCES: S O U R C E S;
QUERIES: Q U E R I E S;


DATA_TYPE: INTEGER_SIGNED_TYPE | INTEGER_UNSIGNED_TYPE | FLOATING_POINT_TYPE | CHAR_TYPE | VARSIZED_TYPE | BOOLEAN_TYPE;

fragment INTEGER_UNSIGNED_TYPE: UNSIGNED_TYPE_QUALIFIER WS INTEGER_BASES_TYPES | U I N T '8' | U I N T '16' | U I N T '32' | U I N T '64';
fragment INTEGER_SIGNED_TYPE: INTEGER_BASES_TYPES | I N T '64' | I N T '32' | I N T '16' | I N T '8';
fragment INTEGER_BASES_TYPES: TINY_INT_TYPE | SMALL_INT_TYPE | NORMAL_INT_TYPE | BIG_INT_TYPE;
fragment TINY_INT_TYPE: T I N Y I N T;
fragment SMALL_INT_TYPE: S M A L L I N T;
fragment NORMAL_INT_TYPE: I N T | I N T E G E R;
fragment BIG_INT_TYPE: B I G I N T;
fragment FLOATING_POINT_TYPE: F L O A T '32' | F L O A T '64';
fragment CHAR_TYPE: C H A R;
fragment VARSIZED_TYPE: V A R S I Z E D;
fragment BOOLEAN_TYPE: B O O L E A N;

fragment UNSIGNED_TYPE_QUALIFIER: U N S I G N E D;

SHOW : S H O W;
FORMAT :  F O R M A T;
CREATE : C R E A T E;
SOURCE : S O U R C E;
LOGICAL: L O G I C A L;
PHYSICAL: P H Y S I C A L;
WORKER: W O R K E R;
SINK : S I N K;

DOT: '.';

/// ThAnK yOu SqL fOr bEiNg CaSe-InSenSiTiVe
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];



SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

/// Make sure that you add lexer rules for keywords before the identifier rule,
// otherwise it will take priority and your grammars will not work
qualifiedIdentifier: IDENTIFIER (DOT IDENTIFIER)*;
identifier: IDENTIFIER;

IDENTIFIER: UNQUOTED_IDENTIFIER | QUOTED_IDENTIFIER;
QUOTED_IDENTIFIER: '"' ( ~'"' )* '"' ;

UNQUOTED_IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    ;

WS: [ \r\n\t]+ -> channel(HIDDEN)
    ;

/// Catch-all for anything we can't recognize.
/// We use this to be able to ignore and recover all the text
/// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
