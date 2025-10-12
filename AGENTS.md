# Repository Guidelines

## Project Structure & Module Organization
- `wacai.py` converts the rooted Android ledger database (`wacai365.so`) into the official `out.xlsx` template stored beside `wacai.xls`.
- `cmb_parser.py` parses CMB credit-card EML statements into `cmb_transactions.xlsx`; parsing helpers sit near the top of the script.
- `unpack_miui_bak/` bundles tools (`miuibak_to_abe.py`, `abe.jar`, `unpack.bat`) for cracking MIUI backups before exporting ledger data.
- Dependencies and metadata live in `pyproject.toml` and `uv.lock`.

## Build, Test, and Development Commands
- `uv sync` — install Python 3.12 dependencies declared in `pyproject.toml`; `pip install -e .` works if `uv` is unavailable.
- `uv run python wacai.py /path/to/wacai365.so` — export the ledger to `out.xlsx` with the official template columns.
- `uv run python cmb_parser.py /path/to/招商银行信用卡电子账单.eml` — pull statement rows into `cmb_transactions.xlsx`.

## Coding Style & Naming Conventions
- Follow PEP 8: four-space indentation, snake_case functions, and descriptive names (see `parse_account` in `wacai.py`).
- Keep scripts runnable from the CLI, and reuse helper functions instead of duplicating parsing logic.
- Preserve existing Chinese column headers and comments; add new UTF-8 literals only when templates require them.

## Testing Guidelines
- There is no automated test suite yet; add `pytest` coverage under `tests/` when extending parsing or formatting logic.
- Use anonymized fixture inputs (`.so`, `.eml`) trimmed to minimal reproductions and assert on DataFrame columns and row counts.
- Run `uv run pytest` (or `pytest` in the active environment) before opening a pull request, noting any intentionally skipped cases.

## Commit & Pull Request Guidelines
- Mirror the concise, imperative history (`adapt to latest wacai database format`, `feat: add borrow and refund`); use prefixes like `feat:` or `fix:` when helpful.
- Keep commits focused, mention related issues in the body, and limit subject lines to 72 characters.
- Pull requests should describe motivation, list manual test commands and resulting files, and include redacted screenshots or workbook diffs for data-facing changes.

## Data Handling Notes
- Mask or anonymize ledger entries before sharing samples in issues or PRs; account exports typically contain personal information.
- Store intermediate dumps outside the repository and extend `.gitignore` for any new secrets or raw exports.
