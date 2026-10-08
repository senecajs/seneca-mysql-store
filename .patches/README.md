# Patches

Changes to `.github/workflows/` need a GitHub token with the `workflow`
scope, so they are delivered here as patches. Apply them from the
repository root:

```sh
git am .patches/*.patch
```

| Patch | Effect |
| ----- | ------ |
| `0001-ci-mysql-service.patch` | `build.yml`: Node 24.x and 22.x matrix, `actions/checkout@v4`, `actions/setup-node@v4`, and a `mysql:9.7` service container on host port 33306 (same as `docker-compose.yml`) with a `mysqladmin ping` health check. The job loads `test/support/db/seed/schema.sql` with the runner's `mysql` client because service containers cannot mount the checkout. |
