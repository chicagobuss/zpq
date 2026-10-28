# Test coverage

Moved from the README for 0.5.0. The check tiers are in [../tools/check/README.md](../tools/check/README.md).

CI ([`.github/workflows/verify.yml`](../.github/workflows/verify.yml)) runs unit tests, Lambda integration tests, CLI
cross-implementation smoke, and the `apache/parquet-testing` conformance corpus on every code change. The smoke path
validates zpq-written Parquet with pyarrow; the workflow also identity-checks the Lambda deploy artifact to catch the
"packaged an incorrect binary" class of bug.
