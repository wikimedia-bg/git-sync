# Repository Guidance

## Safety

- `./git-sync.py` is a live, infinite daemon, not a smoke test. It pulls, commits, pushes, and edits or deletes Wiki pages.
- The script always reads the sibling `config.yml`; there is no CLI or environment override.
- Treat `config.yml` and `git-wiki-sync.service` as operational configuration, not samples. They contain deployment paths,
  repository topology, account details, and attribution mappings.
- The systemd unit passes `resync`, but argument handling is disabled in `git-sync.py`; the argument currently has no effect.

## Validation

- Run `flake8 git-sync.py` from the repository root. `.flake8` sets the non-default 120-character line limit.
- There is no test suite, focused-test command, build, formatter, type checker, or code-generation task.
- Runtime dependencies are unpinned: Python 3.7+, GitPython, Pywikibot, PyYAML, and Git. Do not assume versions or an
  installation command that the repository does not define.

## Runtime Shape

- This is a single-file Python application. `GitSync` wires configured Git repositories to Pywikibot sites; `GitRepo`
  performs each repository's bidirectional synchronization.
- Target repositories must already exist below `repositories_root`, use the hard-coded `master` branch, and have
  authenticated pull and push remotes. The daemon does not clone or initialize them.
- `main()` processes repositories sequentially forever. Each cycle performs Wiki-to-Git before Git-to-Wiki.

## Sync Invariants

- Wiki revisions become individual chronological Git commits and are pushed one at a time.
- Commit messages containing `DO NOT MERGE` or `DO NOT SYNC`, case-insensitively, suppress Git-to-Wiki processing.
- When both sides changed the same file during a cycle, the Wiki change wins; the Git-side file change is discarded and
  a later full resync is scheduled.
- Wiki subpages map from `Page/Subpage` to `Page.d/Subpage`. Forced extensions apply only to root pages.
- Wiki-to-Git writes exactly one trailing newline; Git-to-Wiki strips trailing newlines before saving.
- Edit-summary commit links hard-code the `https://github.com/wikimedia-bg` organization.

## File Conventions

- Python uses a 120-character line limit; no automatic formatter is configured.
- Preserve two-space indentation in `config.yml` manually: `.editorconfig` matches `*.yaml`, not the repository's `*.yml`.
