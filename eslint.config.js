// ESLint flat config for Node-RED node packages.
// Formatting is owned entirely by Prettier (see .prettierrc);
// eslint-config-prettier disables every stylistic rule so the two
// tools never disagree. ESLint's job here is correctness only.
"use strict";

const js = require("@eslint/js");
const globals = require("globals");
const html = require("eslint-plugin-html");
const prettier = require("eslint-config-prettier");

module.exports = [
  // Vendored third-party code (Crockford's cycle.js) - kept
  // byte-identical to upstream, excluded from lint and format.
  { ignores: ["sensor-consensus/cycle.js"] },

  // Runtime + test files: plain CommonJS Node scripts
  {
    files: ["**/*.js"],
    ignores: ["eslint.config.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "commonjs",
      globals: {
        ...globals.node,
      },
    },
    rules: {
      ...js.configs.recommended.rules,
      // Node-RED convention: handler signatures often keep unused
      // parameters (msg, send, done) for documentation; allow them
      // when prefixed with _ or when a later parameter is used.
      "no-unused-vars": [
        "error",
        { args: "after-used", argsIgnorePattern: "^_" },
      ],
      // Catch the classic silent-failure cases
      eqeqeq: ["error", "smart"],
      "no-var": "error",
      "prefer-const": ["warn", { destructuring: "all" }],
    },
  },

  // Editor definition files: JavaScript embedded in <script> blocks.
  // eslint-plugin-html extracts and lints the script contents.
  {
    files: ["**/*.html"],
    plugins: { html },
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "script",
      globals: {
        ...globals.browser,
        ...globals.jquery,
        RED: "readonly",
      },
    },
    rules: {
      ...js.configs.recommended.rules,
      "no-unused-vars": [
        "error",
        { args: "after-used", argsIgnorePattern: "^_" },
      ],
      eqeqeq: ["error", "smart"],
    },
  },

  // This config file itself
  {
    files: ["eslint.config.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "commonjs",
      globals: { ...globals.node },
    },
    rules: { ...js.configs.recommended.rules },
  },

  // Must come last: turns off all rules that conflict with Prettier
  prettier,
];
