import { Lexer } from 'antlr4ng';

export abstract class NesLexerBase extends Lexer {
  isValidDecimal(): boolean {
    const nextChar = this.inputStream.LA(1);
    if (
      (nextChar >= 65 && nextChar <= 90) ||
      (nextChar >= 48 && nextChar <= 57) ||
      nextChar === 95
    ) {
      return false;
    }
    return true;
  }

  isHint(): boolean {
    return this.inputStream.LA(1) === 43; // '+'
  }
}
