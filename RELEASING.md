# Releasing pgcrate

This repo is a single Rust crate (binary) published to crates.io as `pgcrate`.

## Preflight

- Ensure the repo is public (crates.io links to the repository).
- Ensure you are logged into crates.io: `cargo login`.

## Release steps (v0.12.0 example)

1. Update version + changelog
   - Update `Cargo.toml` `package.version`
   - Update `CHANGELOG.md`

2. Run checks locally
   - `cargo fmt`
   - `cargo clippy -- -D warnings`
   - `cargo test`

3. Verify packaging (what will be uploaded)
   - `cargo package`
   - Optional: `cargo publish --dry-run`

4. Publish to crates.io
   - `cargo publish`

5. Tag and push
   - `git tag v0.12.0`
   - `git push origin v0.12.0`

## Notes

- Crate packaging excludes `target/` and `website/` via `Cargo.toml`.
- If you publish from a dirty working tree, use `--allow-dirty` (not recommended for real releases).

