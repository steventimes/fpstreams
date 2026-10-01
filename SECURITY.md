# Security Policy

## Supported versions

Security fixes are provided for supported releases in the 2.x series.

## Reporting a vulnerability

Please report suspected vulnerabilities through GitHub's private vulnerability
reporting feature for this repository. Do not open a public issue before the
report has been assessed.

Include the affected version, a minimal reproduction, the expected impact, and
any known mitigations. We aim to acknowledge reports within seven days.
If the report is confirmed, maintainers will coordinate a fix and disclosure
with you.

## Trust boundary

fpstreams is a local library, not a sandbox. A caller who supplies a filesystem
path is authorizing access to that path, and caller-provided SQL is executed as
the caller's database connection permits. Reports should focus on behavior that
crosses those explicit capabilities, bypasses documented resource limits, or
compromises the build and release chain.

CSV exports preserve raw strings by default. Use `spreadsheet_safe=True` when
opening untrusted text in spreadsheet software. The current source applies this
to header cells too; that header fix is not in the published 2.1.0 wheel.

Atomic file output prevents partial publication and supports no-overwrite writes.
It assumes trusted destination directories and normal filesystem permissions;
it does not isolate a query from processes that can rename those directories.
Spill files contain pickle data and are private temporary state for a query,
not an interchange format for files received from another party.
