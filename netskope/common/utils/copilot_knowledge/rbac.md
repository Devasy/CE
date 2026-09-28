# Cloud Exchange roles & scopes (RBAC)

What each Cloud Exchange security scope grants, and least-privilege recipes for
common job functions. Use this to explain scopes and to draft a scope set for a
new/edited user (admin-only; the admin reviews and Saves — the copilot never
writes users).

## Scopes
- **admin** — full administrative access (manage users, all modules, all
  settings). Grant sparingly; cannot be assigned to brand-new users by design.
- **<module>_read / <module>_write** — read or read+write a module's
  configurations and business rules. `_write` implies the ability to change
  config; pair with `_read`. Modules: `cte` (Threat Exchange), `cto` (Ticket
  Orchestrator / ITSM), `cls` (Log Shipper), `cre` (Risk Exchange), `edm` (Exact
  Data Match), `cfc` (Custom File Classification).
- **settings_read / settings_write** — view or change global/system settings
  (proxy, secrets manager, SSO, tenants, plugin repos, logging level).
- **ai_read** — use the AI Copilot features (analyze, posture assessment,
  configuration copilot) and view AI usage analytics.
- **ai_write** — configure the LLM provider.
- **logs** — view platform/audit logs.
- **api** — create/revoke API tokens.
- **me** — self profile (auto-added to every user).

## Principle
Grant the **least** that lets someone do their job. Prefer `_read` unless the
person actually changes configuration. Reserve `admin` for platform owners.

## Least-privilege recipes
- **CTE operator** (manages threat feeds): `cte_read`, `cte_write`, `logs`,
  and `ai_read` if they should use the copilot. No settings/admin.
- **CTE read-only analyst:** `cte_read`, `logs`, `ai_read`.
- **CTO operator** (manages ticketing): `cto_read`, `cto_write`, `logs`,
  optionally `ai_read`.
- **Settings administrator** (proxy/secrets/tenants, no module ops):
  `settings_read`, `settings_write`.
- **AI administrator** (configures the LLM provider): `ai_read`, `ai_write`.
- **Auditor** (read-only visibility): the relevant `*_read` scopes + `logs`.

> When drafting an `assign_user_scopes` suggestion, list the exact scope strings
> and explain why each is needed; leave username/password to the admin.

## Account Settings (your own profile)

The **Account** dialog — opened from the avatar at the bottom of the sidebar — is
**self-service only**: it acts on the signed-in user, never on anyone else. It shows
the current username and offers two actions.

- **Change Password** — an inline form (current password + new password). The new
  password is checked against the platform **password policy**, which an admin sets
  under **Settings → Users**: minimum/maximum length plus whether an uppercase
  letter, a lowercase letter, a digit and a special character are required. On
  success the session is ended, so the user logs back in with the new password.
  Read the live values with `get_settings('system')` (the `passwordPolicy` key)
  rather than quoting defaults — a deployment can have tightened them.
- **Logout** — ends the session and returns to the login page (it also clears the
  SSO cookies when SSO is enabled).

**Under SSO the Change Password action is not shown.** That is expected, not a bug:
the identity provider owns the credential, so the password is changed there and CE
never sees it. Do not tell an SSO user to change their password in CE.

Anything about OTHER users — creating them, editing their scopes, resetting someone
else's password — is **Settings → Users** and needs `admin`; point the user there
instead of at this dialog.
