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
    if ((nextChar >= 'A' && nextChar <= 'Z') || (nextChar >= 'a' && nextChar <= 'z') || (nextChar >= '0' && nextChar <= '9') ||
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
terminatedStatement: statement SEMICOLON;
multipleStatements: (statement (SEMICOLON statement)* SEMICOLON?)? EOF;
statement: queryWithOptions | createStatement | dropStatement | showStatement | explainStatement;

explainStatement: EXPLAIN (OPEN_PARANTHESIS explainStages CLOSE_PARANTHESIS)? (FORMAT explainFormat)? queryWithSink;
explainStages: explainStage (COMMA explainStage)*;
explainStage: identifier;
explainFormat: identifier;

createStatement: CREATE createDefinition;
createDefinition
    : createLogicalSourceDefinition
    | createPhysicalSourceDefinition
    | createSinkDefinition
    | createWorkerDefinition
    | createModelDefinition
    | createCatalogItemDefinition
    ;

createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                optionsClause?;

createCatalogItemDefinition: type=qualifiedIdentifier name=qualifiedIdentifier optionsClause?;

optionsClause: SET OPEN_PARANTHESIS options=namedConfigLiteralSeq? CLOSE_PARANTHESIS;

namedConfigLiteralSeq: namedConfigLiteral (COMMA namedConfigLiteral)*;
namedConfigLiteral: configLiteral AS qualifiedIdentifier;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier optionsClause?;

createWorkerDefinition: WORKER hostaddr=STRING_LITERAL optionsClause?;

createModelDefinition: MODEL modelName=identifier OPEN_PARANTHESIS modelPath=STRING_LITERAL CLOSE_PARANTHESIS
                       INPUT inputSchema=schemaDefinition
                       OUTPUT outputSchema=schemaDefinition;

schemaCtor: SCHEMA schemaDefinition;
schemaDefinition: OPEN_PARANTHESIS columnDefinition (COMMA columnDefinition)* CLOSE_PARANTHESIS;
columnDefinition: identifier typeDefinition nullableDefinition?;

typeDefinition
    : DATA_TYPE                                                                             # builtinTypeDefinition
    | identifier (OPEN_PARANTHESIS typeParameter (COMMA typeParameter)* CLOSE_PARANTHESIS)? # customTypeDefinition
    ;
typeParameter: typeDefinition | configLiteral;

nullableDefinition: NOT NULL;

dropStatement: DROP dropSubject WHERE dropFilter;
dropSubject: dropQuery | dropSource | dropSink | dropWorker | dropModel;
dropQuery: QUERY;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE;
dropPhysicalSourceSubject: PHYSICAL SOURCE;
dropWorker: WORKER;
dropSink: SINK;
dropModel: MODEL;

dropFilter: attr=identifier EQ value=configScalarLiteral;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT showFormat=identifier)?;
showSubject
    : QUERIES                                                 # showQueriesSubject
    | LOGICAL SOURCES                                         # showLogicalSourcesSubject
    | PHYSICAL SOURCES (FOR logicalSourceName=identifier)?    # showPhysicalSourcesSubject
    | SINKS                                                   # showSinksSubject
    | MODELS                                                  # showModelsSubject
    | VERSION                                                 # showVersionSubject
    | WORKER STATUS                                           # showWorkerStatusSubject
    ;

showFilter: attr=identifier EQ value=configScalarLiteral;

queryWithOptions: queryWithSink optionsClause?;
queryWithSink: unionSeq intoTerm?;
unionSeq: query (unionOp query)*;
unionOp: UNION ALL?;
query: projectionTerm fromTerm filterTerm? (windowTerm groupByTerm? | groupByTerm windowTerm?)? havingTerm?;

projectionTerm: SELECT projectionSeq;
projectionSeq: projection (COMMA projection)*;
projection
    : ASTERISK                                      # asteriskProjection
    | qualifier=identifier DOT ASTERISK             # qualifiedAsteriskProjection
    | openScalarExpression (AS alias=identifier)?   # expressionProjection
    ;

filterTerm: WHERE openScalarExpression;
fromTerm: FROM fromRelation;
fromRelation: closedNamedRelationalExpression | joinRelationalExpression;
groupByTerm
    : GROUP BY (
        OPEN_PARANTHESIS openScalarExpression (COMMA openScalarExpression)* CLOSE_PARANTHESIS
        | openScalarExpression (COMMA openScalarExpression)*
    )
    ;
havingTerm: HAVING openScalarExpression;

intoTerm: INTO sink (COMMA sink)*;
sink: functionCall;

openRelationalExpression: closedRelationalExpression | joinRelationalExpression;
closedRelationalExpression
    : functionCall
    | OPEN_PARANTHESIS openRelationalExpression CLOSE_PARANTHESIS
    | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS
    ;
closedNamedRelationalExpression: closedRelationalExpression (AS alias=identifier)?;

joinRelationalExpression
    : left=closedNamedRelationalExpression inbuildJoins right=closedNamedRelationalExpression ON condition=openScalarExpression binaryWindowTerm
    | leftJoin=joinRelationalExpression inbuildJoins right=closedNamedRelationalExpression ON condition=openScalarExpression binaryWindowTerm
    ;

windowTerm: WINDOW functionCall;
binaryWindowTerm: BINARY_WINDOW functionCall;

inbuildJoins: innerJoin | outerJoinsTypes OUTER? JOIN;
innerJoin: INNER? JOIN;
outerJoinsTypes: LEFT | RIGHT | FULL;

openScalarExpression
    : OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS                                                                                                       # parenthesizedScalarExpression
    | literal                                                                                                                                                       # literalScalarExpression
    | castOp=(CAST | CASTTOTYPE) OPEN_PARANTHESIS openScalarExpression AS targetType=typeDefinition CLOSE_PARANTHESIS                                               # castScalarExpression
    | intervalExpression                                                                                                                                            # intervalScalarExpression
    | qualifier=identifier DOT fieldName=identifier                                                                                                                 # qualifiedFieldAccessExpression
    | functionCall                                                                                                                                                  # functionCallScalarExpression
    | op=(PLUS | MINUS) openScalarExpression                                                                                                                        # unaryArithmeticExpression
    | left=openScalarExpression op=(ASTERISK | DIV | PERCENT) right=openScalarExpression                                                                            # binaryMultiplicativeExpression
    | left=openScalarExpression op=(PLUS | MINUS) right=openScalarExpression                                                                                        # binaryAdditiveExpression
    | left=openScalarExpression op=(EQ | NEQ | LESSER | LESSER_EQUALS | GREATER | GREATER_EQUALS) right=openScalarExpression                                        # binaryComparisonExpression
    | expr=openScalarExpression IS negated=NOT? NULL                                                                                                                # isNullExpression
    | expr=openScalarExpression IS negated=NOT? NAN_                                                                                                                # isNaNExpression
    | expr=openScalarExpression negated=NOT? BETWEEN lower=openScalarExpression AND upper=openScalarExpression                                                      # betweenExpression
    | expr=openScalarExpression negated=NOT? IN OPEN_PARANTHESIS candidates+=openScalarExpression (COMMA candidates+=openScalarExpression)* CLOSE_PARANTHESIS       # inListExpression
    | expr=openScalarExpression negated=NOT? IN OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS                                                                         # unsupportedInSubqueryExpression
    | expr=openScalarExpression IS negated=NOT? kind=(TRUE | FALSE | UNKNOWN)                                                                                       # unsupportedIsTruthExpression
    | expr=openScalarExpression IS negated=NOT? DISTINCT FROM right=openScalarExpression                                                                            # unsupportedIsDistinctExpression
    | expr=openScalarExpression negated=NOT? kind=(LIKE | RLIKE) pattern=openScalarExpression (ESCAPE STRING_LITERAL)?                                              # unsupportedLikeExpression
    | op=(NOT | BANG) openScalarExpression                                                                                                                          # logicalNotExpression
    | left=openScalarExpression AND right=openScalarExpression                                                                                                      # logicalAndExpression
    | left=openScalarExpression OR right=openScalarExpression                                                                                                       # logicalOrExpression
    ;

/// Using named alternatives here because antlr can only resolve mutual left recursion inside a single rule
functionCall
    : identifier                                                                                            # functionReference
    | dataType=DATA_TYPE OPEN_PARANTHESIS (functionCallArgument (COMMA functionCallArgument)*)? CLOSE_PARANTHESIS # dataTypeFunctionCall
    | callee=functionCall OPEN_PARANTHESIS CLOSE_PARANTHESIS                                                # emptyFunctionCall
    | callee=functionCall OPEN_PARANTHESIS ASTERISK CLOSE_PARANTHESIS                                       # starFunctionCall
    | callee=functionCall OPEN_PARANTHESIS functionCallArgument
          (COMMA functionCallArgument)* CLOSE_PARANTHESIS                                                   # positionalFunctionCall
    | callee=functionCall OPEN_PARANTHESIS namedFunctionCallArgument
          (COMMA namedFunctionCallArgument)* CLOSE_PARANTHESIS                                              # namedFunctionCall
    ;

functionCallArgument
    : openScalarExpression                                              # scalarFunctionArg
    | dataType=DATA_TYPE                                                # dataTypeFunctionArg
    | OPEN_PARANTHESIS unionSeq CLOSE_PARANTHESIS                       # subqueryFunctionArg
    | OPEN_PARANTHESIS joinRelationalExpression CLOSE_PARANTHESIS       # joinFunctionArg
    ;

namedFunctionCallArgument: functionCallArgument AS name=qualifiedIdentifier;

intervalExpression
    : INTERVAL value=intervalValue unit=datetimeField
    | SIZE value=intervalValue unit=datetimeField
    | ADVANCE BY value=intervalValue unit=datetimeField
    ;

intervalValue
    : sign=(PLUS | MINUS)? UNSIGNED_INT_LITERAL
    | sign=(PLUS | MINUS)? FLOAT_LITERAL
    | OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS
    ;

datetimeField
    : YEAR | YEARS
    | MONTH | MONTHS
    | DAY | DAYS
    | HOUR | HOURS
    | MINUTE | MINUTES
    | SECOND | SECONDS | SEC
    | MILLISECOND | MILLISECONDS | MS
    ;

literal: scalarLiteral | schemaCtor;
scalarLiteral: boolLiteral | stringLiteral | unsignedIntLiteral | floatLiteral | null;

configLiteral: configScalarLiteral | schemaCtor;
configScalarLiteral: boolLiteral | stringLiteral | signedConfigIntLiteral | signedConfigFloatLiteral | null;
signedConfigIntLiteral: sign=(PLUS | MINUS)? UNSIGNED_INT_LITERAL;
signedConfigFloatLiteral: sign=(PLUS | MINUS)? FLOAT_LITERAL;

boolLiteral: TRUE | FALSE;
stringLiteral: STRING_LITERAL;
unsignedIntLiteral: UNSIGNED_INT_LITERAL;
floatLiteral: FLOAT_LITERAL;
null: NULL;

/// Non-reserved contextual keywords allowed as unquoted identifiers
nonReserved
    : EXPLAIN
    | FORMAT
    | TEXT
    | JSON
    | LOGICAL
    | PHYSICAL
    | SOURCE
    | SOURCES
    | SINK
    | SINKS
    | WORKER
    | STATUS
    | QUERY
    | QUERIES
    | MODEL
    | MODELS
    | VERSION
    | FOR
    | TYPE
    | INPUT
    | OUTPUT
    | WINDOW
    | UNKNOWN
    | INTERVAL
    | SIZE
    | ADVANCE
    | TUMBLING
    | SLIDING
    | YEAR
    | YEARS
    | MONTH
    | MONTHS
    | DAY
    | DAYS
    | HOUR
    | HOURS
    | MINUTE
    | MINUTES
    | SECOND
    | SECONDS
    | SEC
    | MILLISECOND
    | MILLISECONDS
    | MS
    ;

qualifiedIdentifier: identifier (DOT (identifier | SCHEMA))*;
identifier: IDENTIFIER | nonReserved;

/// Punctuation & Operators
SEMICOLON: ';';
OPEN_PARANTHESIS: '(';
CLOSE_PARANTHESIS: ')';
COMMA: ',';
DOT: '.';
ASTERISK: '*';
MINUS: '-';
PLUS: '+';
DIV: '/';
PERCENT: '%';
BANG: '!';
EQ: '=' | '==';
NEQ: '<>' | '!=';
GREATER: '>';
GREATER_EQUALS: '>=' | '!<';
LESSER: '<';
LESSER_EQUALS: '<=' | '!>';

/// Keywords (must precede IDENTIFIER)
EXPLAIN: E X P L A I N;
FORMAT: F O R M A T;
TEXT: T E X T;
JSON: J S O N;
CREATE: C R E A T E;
DROP: D R O P;
SHOW: S H O W;
SET: S E T;
FOR: F O R;
TYPE: T Y P E;
LOGICAL: L O G I C A L;
PHYSICAL: P H Y S I C A L;
SOURCE: S O U R C E;
SOURCES: S O U R C E S;
SINK: S I N K;
SINKS: S I N K S;
WORKER: W O R K E R;
STATUS: S T A T U S;
QUERY: Q U E R Y;
QUERIES: Q U E R I E S;
MODEL: M O D E L;
MODELS: M O D E L S;
VERSION: V E R S I O N;
INPUT: I N P U T;
OUTPUT: O U T P U T;
SCHEMA: S C H E M A;

SELECT: S E L E C T;
FROM: F R O M;
WHERE: W H E R E;
GROUP: G R O U P;
BY: B Y;
HAVING: H A V I N G;
INTO: I N T O;
AS: A S;
ON: O N;
WINDOW: W I N D O W;
BINARY_WINDOW: B I N A R Y '_' W I N D O W;
TUMBLING: T U M B L I N G;
SLIDING: S L I D I N G;

JOIN: J O I N;
INNER: I N N E R;
OUTER: O U T E R;
LEFT: L E F T;
RIGHT: R I G H T;
FULL: F U L L;

UNION: U N I O N;
ALL: A L L;

AND: A N D;
OR: O R;
NOT: N O T;
IS: I S;
NULL: N U L L;
/// NAN_ instead of NAN because math.h defines a NAN macro that would collide in the generated code
NAN_: N A N;
BETWEEN: B E T W E E N;
IN: I N;
LIKE: L I K E;
RLIKE: R L I K E | R E G E X P;
DISTINCT: D I S T I N C T;
ESCAPE: E S C A P E;
TRUE: T R U E;
FALSE: F A L S E;
UNKNOWN: U N K N O W N;

CAST: C A S T;
CASTTOTYPE: C A S T T O T Y P E;

INTERVAL: I N T E R V A L;
SIZE: S I Z E;
ADVANCE: A D V A N C E;
YEAR: Y E A R;
YEARS: Y E A R S;
MONTH: M O N T H;
MONTHS: M O N T H S;
DAY: D A Y;
DAYS: D A Y S;
HOUR: H O U R;
HOURS: H O U R S;
MINUTE: M I N U T E;
MINUTES: M I N U T E S;
SECOND: S E C O N D;
SECONDS: S E C O N D S;
SEC: S E C;
MILLISECOND: M I L L I S E C O N D;
MILLISECONDS: M I L L I S E C O N D S;
MS: M S;

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

STRING_LITERAL: '\'' ( '\'\'' | ~('\''|'\\') | ('\\' .) )* '\'' ;

UNSIGNED_INT_LITERAL: DIGITS;

FLOAT_LITERAL
    : DIGITS EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

fragment DECIMAL_DIGITS
    : DIGITS DOT DIGIT*
    | DOT DIGITS
    ;

fragment EXPONENT
    : E [+-]? DIGITS
    ;

fragment DIGITS: DIGIT+;
fragment DIGIT: [0-9];
fragment LETTER: ('a'..'z'|'A'..'Z'|'_');

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

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT | .)*? '*/' -> channel(HIDDEN)
    ;

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
