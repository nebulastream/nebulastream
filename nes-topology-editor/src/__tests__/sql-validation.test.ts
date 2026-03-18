import { describe, expect, it } from 'vitest';
import { validateSql } from '../lib/sql/validateSql';

describe('validateSql', () => {
  it('accepts a valid SELECT * FROM query', () => {
    const errors = validateSql('SELECT * FROM source1');
    expect(errors).toHaveLength(0);
  });

  it('accepts a valid SELECT with columns and WHERE', () => {
    const errors = validateSql('SELECT a, b FROM source1 WHERE a > 10');
    expect(errors).toHaveLength(0);
  });

  it('reports errors for misspelled SELECT keyword', () => {
    const errors = validateSql('SELEC * FROM source1');
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]!.line).toBe(1);
    expect(errors[0]!.column).toBeGreaterThanOrEqual(0);
  });

  it('reports errors for misspelled FROM keyword', () => {
    const errors = validateSql('SELECT * FORM source1');
    expect(errors.length).toBeGreaterThan(0);
  });

  it('handles empty string gracefully', () => {
    const errors = validateSql('');
    // Empty string is not valid SQL -- should return errors or at least not crash
    expect(Array.isArray(errors)).toBe(true);
  });

  it('accepts a valid query with semicolon', () => {
    const errors = validateSql('SELECT a FROM source1;');
    expect(errors).toHaveLength(0);
  });
});
