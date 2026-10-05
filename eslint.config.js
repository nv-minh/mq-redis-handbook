import js from "@eslint/js";
import prettier from "eslint-config-prettier";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["node_modules/**", "dist/**", "coverage/**", ".worktrees/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,js}"],
    languageOptions: { globals: { ...globals.node } },
  },
  prettier,
);
