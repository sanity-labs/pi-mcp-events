import js from '@eslint/js';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {ignores: ['dist/**', 'node_modules/**', '.local/**', 'coverage/**']},
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.{ts,mjs}'],
    languageOptions: {globals: globals.node},
    rules: {
      eqeqeq: ['error', 'always'],
      'no-nested-ternary': 'error',
      'no-var': 'error',
      'prefer-const': 'error',
      '@typescript-eslint/no-unused-vars': ['error', {argsIgnorePattern: '^_', caughtErrors: 'none'}],
    },
  },
);
