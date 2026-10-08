# Create a release

Goal: publish a new version (maintainers only).

1. Review GitHub issues; close and merge those that belong in the release.
2. Update `CHANGES.md` with the date, version and notes.
3. Check out `master`, clean, and run `npm install`.
4. Run `npm run services:up`, then `npm test`, then `npm run services:down`.
5. Run `npm version x.y.z -m "version x.y.z"`.
6. Push `master` with tags, then run `npm publish --access public`.
7. Draft a GitHub release from the tag with the `CHANGES.md` notes.
8. Notify the core maintainers.
