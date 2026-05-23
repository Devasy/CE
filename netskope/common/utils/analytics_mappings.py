"""Analytics mappings."""

REPOSITORY_MAPPING = {
    "Default": "0",
    "Beta": "1",
    "Crest Hotfix 1": "2",
    "Custom Plugin": "3",
    "Crest Hotfix 2": "4",
    "Custom Repo": "e",
    "Unknown": "f",
}

OS_MAPPING = {
    "Ubuntu 18": "0",
    "Ubuntu 20": "1",
    "Ubuntu 22": "2",
    "Ubuntu 24": "3",
    "Ubuntu": "4",
    "RHEL 7": "5",
    "RHEL 8": "6",
    "RHEL 9": "7",
    "RHEL": "8",
    "CentOS 7": "9",
    "CentOS 8": "a",
    "CentOS": "b",
    "Unknown": "f",
}

MODULES_MAPPING_NUMBERS = {
    "CLS": 1,
    "CTO": 2,
    "CTE": 4,
    "CREV2": 8,
    "EDM": 16,
    "CFC": 32,
}

PLUGINS_STATE_MAPPING = {
    "PULL": 1,
    "SHARE": 2,
    "SYNC": 4,
    "UPDATE": 8,
}

PLUGIN_STATS = {False: "0", True: "1", None: "e"}

HOST_PLATFORM_MAPPING = {
    "vmware": "0",
    "aws": "1",
    "azure": "2",
    "gcp": "3",
    "microsoft": "4",  # Hyper-V
    "custom": "f",
}
