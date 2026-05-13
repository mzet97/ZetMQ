# Security Policy

## Supported versions

Only the latest release is supported for security fixes. Pre-release versions (`0.x.y`) are not covered.

| Version | Supported |
|---------|-----------|
| Latest  | Yes       |
| Older   | No        |

## Reporting a vulnerability

Do not open a public GitHub issue. Report security vulnerabilities to:

**Matheus.zeitune.developer@gmail.com**

Include the following:

1. **Description** -- What the vulnerability is and its impact.
2. **Proof of concept** -- Minimal steps or code to reproduce the issue.
3. **Affected commit** -- The commit hash or version where the issue is present.
4. **Logs** -- Relevant server or client logs, sanitized of sensitive data.

## Response timeline

| Phase | Target |
|-------|--------|
| Acknowledgment | 2 business days |
| Triage and severity assessment | 5 business days |
| Fix (High/Critical) | 30 calendar days |
| Fix (Medium/Low) | Next scheduled release |
| Public disclosure | After fix is released and users have time to upgrade |

Severity is assessed using [CVSS v3.1](https://www.first.org/cvss/v3.1/specification-document).

## Threat model (in scope)

- **Network-layer attacks** -- Unauthorized access to the broker via forged CONNECT frames, credential brute force, or session hijacking.
- **Protocol attacks** -- Malformed frames causing crashes (panic, OOM), denial of service via oversized payloads or unbounded subscriptions.
- **Authentication bypass** -- Circumventing token or username/password auth to publish or subscribe to protected subjects.
- **TLS misconfiguration** -- Clients connecting without TLS when TLS is expected, or accepting invalid certificates in production.
- **Supply chain** -- Compromised dependencies introduced via `crates.io` or git.

## Out of scope

- Denial of service via resource exhaustion on the host machine (CPU, disk, network).
- Vulnerabilities in dependencies that already have a published fix (update your dependencies).
- Physical access attacks or compromise of the host OS.
- Social engineering or phishing.
- Issues in development-only tools (benchmarks, test utilities) that are not part of the runtime.
