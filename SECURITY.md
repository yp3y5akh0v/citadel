# Security Policy

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, use one of the following:

- [GitHub Private Vulnerability Reporting](https://github.com/yp3y5akh0v/citadel/security/advisories/new) (preferred)
- Email: yuriy.peysakhov@gmail.com

Include the following in your report:

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact

## Response

I will acknowledge your report within 5 business days and provide updates as the issue is investigated and resolved.

## Supported Versions

| Version | Supported           |
|---------|---------------------|
| 0.7.x   | Yes                 |
| 0.6.x   | Critical fixes only |
| < 0.6   | No                  |

Critical = data loss, data exposure, cryptographic flaws, or memory-safety bugs. Upgrade to the latest minor before reporting issues in older releases.

## Responsible Disclosure

- Do report vulnerabilities privately using the methods above
- Do provide enough detail to reproduce the issue
- Do not publicly disclose the vulnerability until a fix is available
- Do not exploit the vulnerability beyond what is needed to verify it
