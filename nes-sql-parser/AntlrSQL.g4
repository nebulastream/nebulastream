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

explainStatement: EXPLAIN ('(' explainStages ')')? (FORMAT explainFormat)? queryWithSink;
EXPLAIN: E X P L A I N;
explainStages: explainStage (',' explainStage)*;
explainStage: identifier | LOGICAL;
explainFormat: identifier | TEXT;
TEXT: T E X T;
createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition | createWorkerDefinition | createModelDefinition | createCatalogItemDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                optionsClause?;
createCatalogItemDefinition: type=qualifiedIdentifier name=qualifiedIdentifier optionsClause?;

FOR: F O R;
TYPE: T Y P E;
optionsClause: (SET OPEN_PARANTHESIS options=namedConfigLiteralSeq? CLOSE_PARANTHESIS);
SET: S E T;

namedConfigLiteralSeq: namedConfigLiteral (COMMA namedConfigLiteral)*;
namedConfigLiteral: literal AS qualifiedIdentifier;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier optionsClause?;

createWorkerDefinition: WORKER hostaddr=STRING_LITERAL optionsClause?;

createModelDefinition: MODEL modelName=identifier OPEN_PARANTHESIS modelPath=STRING_LITERAL CLOSE_PARANTHESIS
                       INPUT inputSchema=schemaDefinition
                       OUTPUT outputSchema=schemaDefinition;
MODEL: M O D E L;
INPUT: I N P U T;
OUTPUT: O U T P U T;

schemaCtor: SCHEMA schemaDefinition;
SCHEMA: S C H E M A;
schemaDefinition: OPEN_PARANTHESIS columnDefinition (COMMA columnDefinition)* CLOSE_PARANTHESIS;
columnDefinition: identifier typeDefinition nullableDefinition?;

/// When enabling custom datatypes, take here identifiers in addition
typeDefinition: DATA_TYPE;
nullableDefinition: NOT NULL;
NULL: N U L L;

dropStatement: DROP dropSubject WHERE dropFilter;
DROP: D R O P;
WHERE: W H E R E;
dropSubject: dropQuery | dropSource | dropSink | dropWorker | dropModel;
dropQuery: QUERY;
QUERY: Q U E R Y;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE;
dropPhysicalSourceSubject: PHYSICAL SOURCE;
dropWorker: WORKER;
dropSink: SINK;
dropModel: MODEL;

dropFilter: attr=identifier EQ value=scalarLiteral;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT identifier)?;
showSubject: showQueriesSubject | showLogicalSourcesSubject | showPhysicalSourcesSubject | showSinksSubject | showModelsSubject;
showQueriesSubject: QUERIES;
showLogicalSourcesSubject: LOGICAL SOURCES;
showPhysicalSourcesSubject: PHYSICAL SOURCES;
showSinksSubject: SINKS;
showModelsSubject: MODELS;
MODELS: M O D E L S;
showVersionSubject: VERSION;
VERSION: V E R S I O N;

showFilter: attr=identifier EQ value=scalarLiteral;

queryWithOptions: queryWithSink optionsClause?;
queryWithSink: unionSeq intoTerm;
unionSeq: query (UNION_ALL query)*;
query: projectionTerm fromTerm filterTerm? windowTerm? groupByTerm?;

projectionTerm: SELECT projectionSeq;
SELECT: S E L E C T;
projectionSeq: projection (COMMA projection)*;
projection: ASTERISK | namedExpression;
ASTERISK: '*';
namedExpression: openScalarExpression (AS identifier)?;

filterTerm: WHERE openScalarExpression;
fromTerm: FROM openRelationalExpression;
FROM: F R O M;
groupByTerm: GROUP_BY openScalarExpression (COMMA openScalarExpression)*;
GROUP_BY: GROUP WS BY;
fragment GROUP: G R O U P;
fragment BY: B Y;

intoTerm: INTO sink;
INTO: I N T O;
sink: functionCall;

openRelationalExpression: closedRelationalExpression | joinRelationalExpression;
closedRelationalExpression: functionCall | OPEN_PARANTHESIS openRelationalExpression CLOSE_PARANTHESIS | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS;
AS: A S;
joinRelationalExpression: closedNamedRelationalExpression inbuildJoins closedNamedRelationalExpression ON openScalarExpression;
/// Separated the named version to avoid syntactic ambiguity with named function call parameters.
/// Its only really needed for joins, and if we would want people to be able to just rename the outer-most relation,
// I would rather add this to the evaluation context, i.e. the SELECT, since this matches the named function parameter syntax.
closedNamedRelationalExpression: closedRelationalExpression | closedRelationalExpression AS identifier;


windowTerm: WINDOW functionCall;
//TODO split up the window definitions into the sliding and tumbling ones, maybe even make them function call, because they require different amounts of parameters
//binaryWindowExpression: WINDOW windowType OPEN_PARANTHESIS IDENTIFIER COMMA IDENTIFIER COMMA intervalExpression CLOSE_PARANTHESIS;
//unaryWindowExpression: WINDOW windowType OPEN_PARANTHESIS IDENTIFIER COMMA intervalExpression CLOSE_PARANTHESIS;
//windowType: TUMBLING | SLIDING;
//TUMBLING: T U M B L I N G;
//SLIDING: S L I D I N G;
WINDOW: W I N D O W;
ON: O N;
joinOp: inbuildJoins | functionCall;
inbuildJoins: innerJoin | outerJoinsTypes OUTER? JOIN;
innerJoin: INNER? JOIN;
outerJoinsTypes: LEFT | RIGHT | FULL;
LEFT: L E F T;
RIGHT: R I G H T;
FULL: F U L L;
OUTER: O U T E R;
INNER: I N N E R WS;
JOIN: J O I N;

UNION_ALL: UNION ALL?;
fragment ALL: A L L;
fragment UNION: U N I O N;

openScalarExpression: infixScalarExpression | openScalarExpression scalarPostfixes | intervalExpression | closedScalarExpression;
closedScalarExpression: literal
    | prefixScalarExpression
    | castFunctionalScalarExpression
    | functionCall
    | OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS
    | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS;

prefixScalarExpression: prefixScalarOperator closedScalarExpression;
infixScalarExpression: closedScalarExpression infixScalarOperator closedScalarExpression;
castFunctionalScalarExpression: DATA_TYPE OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS;


/// Using named alternatives here because antlr can only resolve mutual left recursion inside a single rule
functionCall
    : identifier                                                                   # functionReference
    | functionCall OPEN_PARANTHESIS positionalFunctionCallArgument
          (COMMA positionalFunctionCallArgument)* CLOSE_PARANTHESIS                # positionalFunctionCall
    | functionCall OPEN_PARANTHESIS namedFunctionCallArgument
          (COMMA namedFunctionCallArgument)* CLOSE_PARANTHESIS                     # namedFunctionCall
    ;
positionalFunctionCallArgument: closedScalarExpression | closedRelationalExpression;
namedFunctionCallArgument: positionalFunctionCallArgument AS qualifiedIdentifier;

/// Once we have better literal folding, this separate block can be removed and its usages be replaced by functionCall
//literalFunctionCall: positionalLiteralFunctionCall | namedLiteralFunctionCall;
//positionalLiteralFunctionCall: identifier OPEN_PARANTHESIS positionalLiteralFunctionCallArgument (COMMA positionalLiteralFunctionCallArgument)* CLOSE_PARANTHESIS;
//positionalLiteralFunctionCallArgument: literal;
//namedLiteralFunctionCall: identifier OPEN_PARANTHESIS namedLiteralFunctionCallArgument (COMMA namedLiteralFunctionCallArgument)* CLOSE_PARANTHESIS;
//namedLiteralFunctionCallArgument: positionalLiteralFunctionCallArgument AS qualifiedIdentifier;

OPEN_PARANTHESIS: '(';
CLOSE_PARANTHESIS: ')';
COMMA: ',';

//intervalLiteral: INTERVAL literal DATETIME_FIELD;
intervalExpression: INTERVAL openScalarExpression datetimeField;
datetimeField: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECOND;
YEAR: Y E A R;
MONTH: M O N T H;
DAY: D A Y;
HOUR: H O U R;
MINUTE: M I N U T E;
SECOND: S E C O N D;
MILLISECOND: M I L L I S E C O N D;

INTERVAL: I N T E R V A L;


/// NAN_ instead of NAN because math.h defines a NAN macro that would collide in the generated code
scalarPostfixes: IS NULL | IS NAN_;
IS: I S;
NAN_: N A N;

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
GREATER_EQUALS: '>=' | '!<';
LESSER: '<';
LESSER_EQUALS: '<=' | '!>';


literal: scalarLiteral | schemaCtor;
scalarLiteral: boolLiteral | stringLiteral | signedIntLiteral | floatLiteral | null;

boolLiteral: TRUE | FALSE;
TRUE: T R U E;
FALSE: F A L S E;

stringLiteral: STRING_LITERAL;
STRING_LITERAL: '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' ;

signedIntLiteral: SIGNED_INT_LITERAL;
SIGNED_INT_LITERAL: MINUS? DIGITS;

floatLiteral: decimalFraction floatExponent?;
floatExponent: E MINUS? exponent=DIGITS;
decimalFraction: MINUS? integers=DIGITS DOT fraction=DIGIT* | MINUS? DOT fraction=DIGITS;

DIGITS: DIGIT+;
DIGIT : [0-9] ;

null: NULL;

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
E : [eE];
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
