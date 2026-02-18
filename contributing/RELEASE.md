# Release Workflow

This document outlines the steps to create a new release for the `pyspark-data-sources` project.

## Prerequisites

- Ensure you have [uv](https://docs.astral.sh/uv/) installed
- Ensure you have GitHub CLI installed (optional, for enhanced releases)
- Ensure you have push access to the repository
- Ensure all tests pass and the code is ready for release

## Release Steps

### 1. Update Version

`uv` does not manage versions directly, but you can use the Hatch CLI via `uvx` or edit `pyproject.toml` manually.

```bash
# Bump patch version (0.1.6 → 0.1.7) - for bug fixes
uvx hatch version patch

# Bump minor version (0.1.6 → 0.2.0) - for new features
uvx hatch version minor

# Bump major version (0.1.6 → 1.0.0) - for breaking changes
uvx hatch version major

# Or set a specific version
uvx hatch version 1.2.3
```

These commands update the `version` field under `[project]` in `pyproject.toml`. You can also open the file and edit the value manually if you prefer.

### 2. Build and Publish

```bash
# Build the package (creates dist/ artifacts)
uv build

# Publish to PyPI (requires token or username/password)
uv publish

# Optional: dry-run to verify upload without publishing
uv publish --dry-run
```

### 3. Commit Version Changes

```bash
# Add the version change
git add pyproject.toml

# Commit with the current version (automatically retrieved)
VERSION=$(uvx hatch version)
git commit -m "Bump version to ${VERSION}"

# Push to main branch
git push
```

### 4. Create GitHub Release

#### Option A: Simple Git Tag
```bash
# Create an annotated tag with current version
VERSION=$(uvx hatch version)
git tag -a "v${VERSION}" -m "Release version ${VERSION}"

# Push the tag to GitHub
git push origin "v${VERSION}"
```

#### Option B: Rich GitHub Release (Recommended)
```bash
# Create a GitHub release with current version
VERSION=$(uvx hatch version)
gh release create "v${VERSION}" \
  --title "Release v${VERSION}" \
  --notes "Release notes for version ${VERSION}" \
  --latest
```

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):

- **Patch** (`uvx hatch version patch`): Bug fixes, no breaking changes
- **Minor** (`uvx hatch version minor`): New features, backward compatible  
- **Major** (`uvx hatch version major`): Breaking changes

## Manual Version Update (Alternative)

If you prefer to manually edit `pyproject.toml`:

```toml
[project]
version = "x.y.z"  # Update this line manually
```

Then follow steps 2-4 above.

## Release Checklist

- [ ] All tests pass
- [ ] Documentation is up to date
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Version is bumped using `uvx hatch version [patch|minor|major]` (or manual edit)
- [ ] Package builds successfully (`uv build`)
- [ ] Package publishes successfully (`uv publish`)
- [ ] Version changes are committed and pushed
- [ ] GitHub release/tag is created
- [ ] Release notes are written

## Troubleshooting

### Publishing Issues
- Ensure you're authenticated with PyPI: `uv publish --token <pypi-token>` or set credentials via environment variables
- Check if the version already exists on PyPI

### Git Tag Issues
- If tag already exists:

```bash
VERSION=$(uvx hatch version)
git tag -d "v${VERSION}"
git push origin :refs/tags/"v${VERSION}"
```
- Ensure you have push permissions to the repository

### GitHub CLI Issues
- Authenticate: `gh auth login`
- Check repository access: `gh repo view`

### PyArrow Compatibility Issues

If you see `objc_initializeAfterForkError` crashes on macOS, set this environment variable:

```bash
# For single commands
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python your_script.py

# For commands that run inside the uv environment
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES uv run python your_script.py

# To set permanently in your shell (add to ~/.zshrc or ~/.bash_profile):
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

## Useful Version Commands

```bash
# Check current version
uvx hatch version

# Bump to an explicit version
uvx hatch version 0.2.0
```

## Documentation

Documentation lives as Markdown files in the repository and is the source of truth. No separate build or deployment is needed.

### Documentation Structure

- **README.md** – Installation, usage, and data source table
- **examples/** – Copy-pastable examples for each data source
- **docs/** – Guides (data-sources-guide.md, building-data-sources.md, api-reference.md)

### Adding or Updating Documentation

1. Edit the relevant `.md` files in the repo
2. Add new examples to `examples/` (see `examples/README.md` for the index)
3. Push to main/master branch
