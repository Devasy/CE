# CE deployment options (how Cloud Exchange can be deployed)

Cloud Exchange can be deployed in several shapes. Any deployment is described by **four
independent axes** — platform provider, host OS, deployment type, and flavour. This pack is
the *option matrix* (what the possible values are and what each means). For what THIS host
actually is, call **`get_deployment_details`** — it reads the live deployment environment and
works from any page. Use both together: report the live values, then explain them from here.

## The four axes

### Platform Provider
Where the host runs. Detected at install/upgrade time by the `setup` script (cloud metadata
endpoints for the three clouds, the hypervisor vendor otherwise) and stored as
`PLATFORM_PROVIDER`.

| Value | Means |
|---|---|
| `gcp` | Google Cloud Platform |
| `aws` | Amazon Web Services |
| `azure` | Microsoft Azure |
| `vmware` | VMware (ESXi / vSphere) |
| `microsoft` | Microsoft Hyper-V — the stored value is literally `microsoft`, not "hyperv" |
| `custom` | Anything else: bare metal, an unrecognised hypervisor, or a cloud CE could not identify. Also the default when detection fails. |

### Host OS
The OS family **plus major version**, e.g. `Ubuntu 24`, `RHEL 9`. Only two families are
supported: **Ubuntu** and **RHEL (Red Hat Enterprise Linux)**. `Ubuntu 24` and `RHEL 9` are the
recommended versions; older values (`Ubuntu 22`, `RHEL 8`, …) show up on hosts upgraded from
earlier releases. `Unknown` means detection did not run or failed — not that the OS is
unsupported.

### Deployment Type
- **Standalone (SA)** — the whole stack runs on a single host. The default.
- **HA (High Availability)** — a multi-node cluster (MongoDB replica set, RabbitMQ and shared
  storage across the nodes). Set when a node list was configured at setup time.

### Flavour
- **Container** — CE containers on a customer-provided host, brought up with the
  docker-compose stack. The user installs and owns the OS.
- **VM (CE as VM)** — a **pre-built CE appliance image** with CE already baked in: OVA
  (VMware), VHDX (Hyper-V), an AWS AMI, an Azure image, or a GCP image. The user imports the
  image instead of installing onto their own host. Appliance images are always Ubuntu-based.

## Reading the combination

- The axes are independent, so a deployment is a *combination*: e.g. "AWS / Ubuntu 24 / HA /
  Container", or "VMware / Ubuntu 24 / Standalone / VM (OVA appliance)".
- **Flavour = VM narrows the rest.** The appliance images are Ubuntu-based, and the platform
  provider will be whichever image target it was imported into (vmware, microsoft, aws, azure,
  gcp). A `RHEL` + `VM` combination does not exist.
- **Platform Provider `custom` is normal**, not an error — it just means CE did not recognise a
  cloud or a supported hypervisor. Never present it as a misconfiguration.
- **HA is orthogonal to flavour**: an HA cluster can be built from appliance VMs or from
  containers on customer hosts.

## Why it matters when advising

Deployment-sensitive guidance — upgrades, HA node work, disk/storage layout, OS-level steps
(package manager, firewall, SELinux vs AppArmor), backup/restore and appliance-vs-compose
paths — differs by these axes. **Check `get_deployment_details` before giving steps that
depend on them**, rather than assuming a standalone Ubuntu container install:

- On **HA**, host-level actions usually have to be repeated per node, and taking one node down
  is not the same as taking the deployment down.
- On the **VM** flavour, the user does not manage a separate host OS install the way a
  Container deployment does, so "install docker / edit the compose file" advice may not fit.
- On **RHEL**, OS-level commands differ from Ubuntu (`dnf`/`yum` vs `apt`, `firewalld` vs
  `ufw`, SELinux vs AppArmor).

## Where the user sees this

- **System Health** page → the **System Specifications** panel shows Platform Provider, Host OS,
  Deployment Type and Flavor exactly as `get_deployment_details` reports them.
- The same values back `GET /api/ce-details`. The copilot should NOT send the user to the API —
  point them at System Health, and answer with the tool.
