# Release Workflow

This document outlines the steps to create a new release for the `pyspark-data-sources` project.

## Prerequisites

- Ensure you have Poetry installed
- Ensure you have GitHub CLI installed (optional, for enhanced releases)
- Ensure you have push access to the repository
- Ensure all tests pass and the code is ready for release

## Release Steps

### 1. Update Version (Using Poetry Commands)

Use Poetry's built-in version bumping commands:

```bash
# Bump patch version (0.1.6 → 0.1.7) - for bug fixes
poetry version patch

# Bump minor version (0.1.6 → 0.2.0) - for new features
poetry version minor

# Bump major version (0.1.6 → 1.0.0) - for breaking changes
poetry version major

# Or set a specific version
poetry version 1.2.3
```

This automatically updates the version in `pyproject.toml`.

### 2. Build and Publish

```bash
# Build the package
poetry build

# Publish to PyPI (requires PyPI credentials)
poetry publish
```

### 3. Commit Version Changes

```bash
# Add the version change
git add pyproject.toml

# Commit with the current version (automatically retrieved)
git commit -m "Bump version to $(poetry version -s)"

# Push to main branch
git push
```

### 4. Create GitHub Release

#### Option A: Simple Git Tag
```bash
# Create an annotated tag with current version
git tag -a "v$(poetry version -s)" -m "Release version $(poetry version -s)"

# Push the tag to GitHub
git push origin "v$(poetry version -s)"
```

#### Option B: Rich GitHub Release (Recommended)
```bash
# Create a GitHub release with current version
gh release create "v$(poetry version -s)" \
  --title "Release v$(poetry version -s)" \
  --notes "Release notes for version $(poetry version -s)" \
  --latest
```

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):

- **Patch** (`poetry version patch`): Bug fixes, no breaking changes
- **Minor** (`poetry version minor`): New features, backward compatible  
- **Major** (`poetry version major`): Breaking changes

## Manual Version Update (Alternative)

If you prefer to manually edit `pyproject.toml`:

```toml
[tool.poetry]
version = "x.y.z"  # Update this line manually
```

Then follow steps 2-4 above.

## Release Checklist

- [ ] All tests pass
- [ ] Documentation is up to date
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Version is bumped using `poetry version [patch|minor|major]`
- [ ] Package builds successfully (`poetry build`)
- [ ] Package publishes successfully (`poetry publish`)
- [ ] Version changes are committed and pushed
- [ ] GitHub release/tag is created
- [ ] Release notes are written

## Troubleshooting

### Publishing Issues
- Ensure you're authenticated with PyPI: `poetry config pypi-token.pypi your-token`
- Check if the version already exists on PyPI

### Git Tag Issues
- If tag already exists: `git tag -d "v$(poetry version -s)"` (delete local) and `git push origin :refs/tags/"v$(poetry version -s)"` (delete remote)
- Ensure you have push permissions to the repository

### GitHub CLI Issues
- Authenticate: `gh auth login`
- Check repository access: `gh repo view`

### PyArrow Compatibility Issues

If you see `objc_initializeAfterForkError` crashes on macOS, set this environment variable:

```bash
# For single commands
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python your_script.py

# For Poetry environment
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES poetry run python your_script.py

# To set permanently in your shell (add to ~/.zshrc or ~/.bash_profile):
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

## Useful Poetry Version Commands

```bash
# Check current version
poetry version

# Show version number only (useful for scripts)
poetry version -s

# Preview what the next version would be (without changing it)
poetry version --dry-run patch
poetry version --dry-run minor
poetry version --dry-run major
```

## Documentation Releases

The project uses MkDocs with GitHub Pages for documentation. The documentation is automatically built and deployed via GitHub Actions.

### Automatic Documentation Updates

The docs workflow (`.github/workflows/docs.yml`) automatically triggers when you push changes to:
- `docs/**` - Any documentation files
- `mkdocs.yml` - MkDocs configuration  
- `pyproject.toml` - Project configuration (version updates)
- `.github/workflows/docs.yml` - The workflow itself

### Manual Documentation Deployment

You can manually trigger documentation deployment:

```bash
# Using GitHub CLI
gh workflow run docs.yml

# Or trigger via GitHub web interface:
# Go to Actions tab → Deploy MkDocs to GitHub Pages → Run workflow
```

### Documentation URLs

- **Live Docs**: https://allisonwang-db.github.io/pyspark-data-sources
- **Source**: `docs/` directory
- **Configuration**: `mkdocs.yml`
- **Workflow**: `.github/workflows/docs.yml`

### Adding New Documentation

1. Create new `.md` files in `docs/` or `docs/datasources/`
2. Update `mkdocs.yml` navigation if needed
3. Push to main/master branch
4. Documentation will auto-deploy via GitHub Actions
