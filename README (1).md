# Tests for static checks
This folder contains intentionally-buggy files to trigger a variety of static checks:

Categories included:
1) Syntax & Parsing: check_syntax.py, check_parse_errors.sql, check_notebook_syntax.ipynb
2) Security & Compliance: secrets_and_connections.json, adf_pipeline_buggy.json
3) Code Quality & Maintainability: naming_and_quality.py, code_duplication.py, dead_code.py
4) ADF Pipeline Hygiene: adf_pipeline_buggy.json (missing KeyVault refs, hardcoded secrets, no managed identity)

Use these files to exercise your scanners and checkers. Files intentionally include:
- Syntax errors, malformed JSON/SQL, and notebook cell errors
- Hardcoded keys, tokens, connection strings
- PII examples (emails, phone numbers, SSNs) in notebook
- Naming convention problems, duplicate code, dead code, and high complexity
