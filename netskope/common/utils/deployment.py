"""Deployment topology of THIS Cloud Exchange install, read from the environment.

CE ships in several shapes; four axes distinguish them — the platform it runs on, the
host OS, standalone vs HA, and container vs appliance VM. The stack's ``setup`` script
detects all four at install/upgrade time (``collect_host_details``), writes them into the
compose ``.env``, and compose injects them into every container — so reading
``os.environ`` here is the authoritative, zero-cost source: no DB round-trip, no HTTP
call. ``GET /api/ce-details`` and the copilot's ``get_deployment_details`` tool both call
``deployment_details()`` so the two can never drift.

Nothing here is a secret — it is host/platform metadata. ``HA_IP_LIST`` is only tested
for PRESENCE; the node IPs it holds never leave this module.
"""

import os

# PLATFORM_PROVIDER: what the host runs on. Written by setup's host detection —
# cloud metadata endpoints for gcp/aws/azure, the lscpu hypervisor vendor for
# vmware/microsoft, else "custom". Keys mirror HOST_PLATFORM_MAPPING in
# analytics_mappings.py (the wire format already reported to Netskope analytics), so a
# new provider must be added in BOTH places.
PLATFORM_PROVIDER_LABELS = {
    "gcp": "Google Cloud Platform",
    "aws": "Amazon Web Services",
    "azure": "Microsoft Azure",
    "vmware": "VMware",
    "microsoft": "Microsoft Hyper-V",
    "custom": "Custom / self-managed host (no cloud or hypervisor detected)",
}

# HOST_OS: the OS family + MAJOR version, e.g. "Ubuntu 24" / "RHEL 9". Only these two
# families are supported (see RECOMMENDED_HOST_OS in the compose repo); the appliance
# images Diskbot builds are always Ubuntu-based.
HOST_OS_FAMILIES = {
    "Ubuntu": "Ubuntu Linux — the value carries the major version, e.g. 'Ubuntu 24'.",
    "RHEL": "Red Hat Enterprise Linux — the value carries the major version, e.g. 'RHEL 9'.",
}

# HA_IP_LIST present => the node list was configured => HA.
DEPLOYMENT_TYPES = {
    "Standalone": "Standalone (SA) — the whole stack runs on a single host.",
    "HA": "High Availability — a multi-node cluster (MongoDB replica set + RabbitMQ across nodes).",
}

# CE_AS_VM=True is stamped by setup when the /.cloud_exchange_vm.marker file is present,
# i.e. the host IS a pre-built CE appliance image rather than a customer host.
FLAVORS = {
    "Container": "Containers on a customer-provided host.",
    "VM": "CE as VM — the pre-built appliance image (OVA / VHDX / AMI / Azure / GCP), CE already baked in.",
}

# The whole option matrix in one serialisable dict, for callers that need to state what
# the possible values ARE (the copilot tool) and not just what this host happens to be.
DEPLOYMENT_OPTIONS = {
    "platformProvider": PLATFORM_PROVIDER_LABELS,
    "hostOS": HOST_OS_FAMILIES,
    "deploymentType": DEPLOYMENT_TYPES,
    "flavor": FLAVORS,
}


def _env(name: str, default: str = "") -> str:
    """Read an env var, stripping whitespace and the quotes setup writes into .env."""
    return os.environ.get(name, default).strip().strip('"')


def deployment_details() -> dict:
    """Return this deployment's four axes, keyed exactly as ``/api/ce-details`` reports them.

    Returns:
        dict: ``Platform Provider`` (gcp/aws/azure/vmware/microsoft/custom), ``Host OS``
        (family + major version), ``Deployment Type`` (Standalone/HA) and ``Flavor``
        (Container/VM). Keys are the display labels the System Health page renders
        as-is — do NOT rename them without updating that page.
    """
    return {
        "Platform Provider": _env("PLATFORM_PROVIDER", "custom"),
        "Host OS": _env("HOST_OS", "Unknown"),
        "Deployment Type": "HA" if os.environ.get("HA_IP_LIST") else "Standalone",
        "Flavor": "VM" if _env("CE_AS_VM", "False").lower() == "true" else "Container",
    }
