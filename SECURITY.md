# Security

## Reporting a vulnerability

Please don't open a public issue for a security problem. Report it privately through GitHub:
**Security → Report a vulnerability** on this repository
([direct link](https://github.com/Ash20pk/IsomerDB/security/advisories/new)).

Say what's affected, how to reproduce it, and what an attacker could do with it. You'll hear back
within a week. Once there's a fix, it goes out in a release with an advisory, and you're credited
unless you'd rather not be.

## Supported versions

Fixes go into the latest release. IsomerDB is young, so older releases don't get backports: upgrade
to the latest (see [docs/self-hosting.md](docs/self-hosting.md#upgrading)).

## Problems in upstream code

Citus, DocumentDB and pg_trickle are built from source in [`extensions/`](extensions/). If the
problem is in their code rather than ours, report it to that project as well. We'll ship their fix,
or our own, in an IsomerDB release.

## What's in scope

The `isomer` extension, the HTTP API and console, the MongoDB gateway changes, the edge replica,
the embedder, the operator, and the images we publish. The Docker Compose setup's fixed passwords
and password-less workers are for local development and are documented as such, so they're not
vulnerabilities.
