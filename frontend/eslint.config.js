import { defineConfig, globalIgnores } from 'eslint/config'
import js from '@eslint/js'
import globals from 'globals'
import tseslint from 'typescript-eslint'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import react from 'eslint-plugin-react'
import jsxA11y from 'eslint-plugin-jsx-a11y'

const eslintConfig = defineConfig([
  // Override default ignores
  globalIgnores([
    'dist',
    'build',
    'node_modules/**/*.*',
    '.vite/**',
  ]),
  {
    files: ['src/**/*.{js,jsx,ts,tsx}', 'test/**/*.{js,jsx,mjs}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: {
        // Browser globals
        window: 'readonly',
        document: 'readonly',
        navigator: 'readonly',
        // Node globals
        process: 'readonly',
        __dirname: 'readonly',
        __filename: 'readonly',
        module: 'readonly',
        require: 'readonly',
        // Jest/Vitest globals
        describe: 'readonly',
        test: 'readonly',
        it: 'readonly',
        expect: 'readonly',
        beforeEach: 'readonly',
        afterEach: 'readonly',
        beforeAll: 'readonly',
        afterAll: 'readonly',
        vi: 'readonly',
        ...globals.browser,
      },
    },
    plugins: {
      react,
      'jsx-a11y': jsxA11y,
    },
    settings: {
      react: {
        version: 'detect',
      },
    },
    rules: {
      // Spacing and formatting
      'keyword-spacing': ['error', { before: true, after: true }],
      'newline-per-chained-call': 'off',
      'indent': [
        'error',
        2,
        {
          ignoredNodes: ['TemplateLiteral'],
          SwitchCase: 1,
        },
      ],
      'no-multi-spaces': [
        'warn',
        {
          ignoreEOLComments: true,
        },
      ],
      'object-curly-newline': [
        'error',
        {
          consistent: true,
        },
      ],
      'object-curly-spacing': ['error', 'always'],
      'operator-linebreak': ['warn', 'before'],
      'template-curly-spacing': 'off',

      // Code quality
      '@typescript-eslint/no-explicit-any': 'off',
      'arrow-parens': ['error', 'as-needed'],
      'comma-dangle': ['error', 'only-multiline'],
      'consistent-return': 'warn',
      'default-param-last': 'warn',
      'eqeqeq': 'off',
      'max-len': 'off',
      'no-console': [
        'error',
        {
          allow: ['warn', 'error'],
        },
      ],
      'no-var': 'error',
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          varsIgnorePattern: 'logger|ignore|_',
          argsIgnorePattern: 'logger|ignore|_',
          args: 'after-used',
          ignoreRestSiblings: true,
        },
      ],
      'no-duplicate-imports': 'error',
      'no-param-reassign': [
        'warn',
        {
          props: false,
        },
      ],
      'no-nested-ternary': 'error',
      'no-unused-expressions': 'error',
      'prefer-const': 'error',
      'prefer-destructuring': 'off',
      'camelcase': 'off',
      'new-cap': 'off',
      'no-invalid-this': 'error',

      // Quotes and semicolons
      'quotes': [
        'error',
        'single',
        {
          avoidEscape: true,
        },
      ],
      'semi': [
        'error',
        'never',
        {
          beforeStatementContinuationChars: 'always',
        },
      ],

      // Restricted usage
      'no-restricted-globals': [
        'error',
        {
          name: 'window',
          message: 'Avoid using window directly. Use utility functions for global access.',
        },
      ],
      'no-restricted-imports': [
        'error',
        {
          patterns: [],
        },
      ],
      'no-restricted-properties': [
        'error',
        {
          property: 'debugForce',
          message: 'Do not use debugForce in production.',
        },
      ],

      // React rules
      'react/react-in-jsx-scope': 'off',
      'react/jsx-key': 'off',
      'react/destructuring-assignment': 'off',
      'react/forbid-prop-types': 'warn',
      'react/function-component-definition': [
        'error',
        {
          namedComponents: 'arrow-function',
          unnamedComponents: 'arrow-function',
        },
      ],
      'react/jsx-filename-extension': [
        'error',
        {
          extensions: ['.jsx', '.tsx'],
        },
      ],
      'react/jsx-no-duplicate-props': [
        'error',
        {
          ignoreCase: false,
        },
      ],
      'react/jsx-one-expression-per-line': 'off',
      'react/jsx-props-no-spreading': 'off',
      'react/prop-types': [
        'warn',
        {
          ignore: [
            'aria-label',
            'aria-labelledby',
            'classes',
            'className',
            'data-testid',
            'ignore',
            'role',
            'theme',
            'width',
          ],
          skipUndeclared: true,
        },
      ],
      'react/no-multi-comp': 'off',
      'react/require-default-props': [
        'warn',
        {
          functions: 'defaultArguments',
        },
      ],
      'react/sort-comp': [
        'warn',
        {
          order: [
            'static-variables',
            'static-methods',
            'instance-variables',
            'lifecycle',
            'getters',
            'setters',
            '/^(handle|on).+$/',
            'everything-else',
            'render',
          ],
        },
      ],
      'react/state-in-constructor': 'off',
      'react/static-property-placement': 'off',

      // React Hooks
      'react-hooks/rules-of-hooks': 'error',
      'react-hooks/exhaustive-deps': 'warn',

      // JSX A11y
      'jsx-a11y/label-has-associated-control': [
        'error',
        {
          required: {
            some: ['nesting', 'id'],
          },
        },
      ],
    },
  },
])

export default eslintConfig
