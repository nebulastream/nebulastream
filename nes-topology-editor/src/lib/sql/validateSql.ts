import { CharStream, CommonTokenStream } from 'antlr4ng';
import { AntlrSQLLexer } from './AntlrSQLLexer.js';
import { AntlrSQLParser } from './AntlrSQLParser.js';

export interface SqlError {
  line: number;
  column: number;
  message: string;
}

export function validateSql(sql: string): SqlError[] {
  const inputStream = CharStream.fromString(sql);
  const lexer = new AntlrSQLLexer(inputStream);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new AntlrSQLParser(tokenStream);

  const errors: SqlError[] = [];
  const collector = {
    syntaxError(
      _recognizer: unknown,
      _offendingSymbol: unknown,
      line: number,
      column: number,
      msg: string,
    ) {
      errors.push({ line, column, message: msg });
    },
    reportAmbiguity() {},
    reportAttemptingFullContext() {},
    reportContextSensitivity() {},
  };

  lexer.removeErrorListeners();
  parser.removeErrorListeners();
  lexer.addErrorListener(collector);
  parser.addErrorListener(collector);

  parser.singleStatement();

  return errors;
}
