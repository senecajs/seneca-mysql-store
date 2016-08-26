# Creating a release

1.  Review github issues, triage, close and merge issues related to the release.
2.  Update CHANGES.md, with date release, notes, and version.
3.  Pull down the repository locally on the master branch.
4.  Ensure there are no outstanding commits and the branch is clean.
5.  Run `npm install` and ensure all dependencies correctly install.
6.  Run `npm run build` to build the Docker container.
7.  Run `npm run start` to start the mysql Docker container.
8.  Run `npm run test` and ensure testing and linting passes.
9.  Run `npm version vx.x.x -m "version x.x.x"` where `x.x.x` is the version.
10.  Run `git push upstream master --tags`
11.  Run `npm publish`
12. Go to the [Github release page][Releases] and hit 'Draft a new release'.
13. Paste the Changelog content for this release and add additional release notes.
14. Choose the tag version and a title matching the release and publish.
15. Notify core maintainers of the release via email.

[Releases]: https://github.com/senecajs/seneca-mysql-store/releases