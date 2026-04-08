"""Migration related package."""

from netskope.common.utils import DBConnector, Collections
from netskope.common.utils.notifier import Notifier

connector = DBConnector()
notifier = Notifier()


def iterator_migration():
    """Iterate all the tenant for migration."""
    tenants = connector.collection(Collections.NETSKOPE_TENANTS).find({})
    tenant_dict = {}
    for tenant in tenants:
        if tenant.get("v2token") is None:
            tenant_dict[tenant.get("name")] = (
                f"[{tenant.get('name')}](https://{tenant.get('tenantName')}.goskope.com/ns#/settings)"
                if "https" not in tenant.get("tenantName")
                else f"[{tenant.get('name')}]({tenant.get('tenantName')}/ns#/settings)"
            )
    if tenant_dict:
        notifier.banner_error(
            id="BANNER_ERROR_1000",
            message=f"Configure tenant(s) **{', '.join(f'{value}' for key, value in tenant_dict.items())}** "
            f"with V2 token. Navigate to Settings > Netskope Tenants to update tenants with V2 token. ",
        )
        connector.collection(Collections.NOTIFICATIONS).update_one(
            {"id": "BANNER_ERROR_1000"},
            {
                "$set": {
                    "acknowledged": False,
                },
            },
            upsert=True,
        )
        print(
            f"Configure tenant(s) {', '.join(f'{value}' for key, value in tenant_dict.items())} with V2 token."
            f" Navigate to Settings > Netskope Tenants to update tenants with V2 token."
        )
    else:
        connector.collection(Collections.NOTIFICATIONS).update_one(
            {"id": "BANNER_ERROR_1000"},
            {
                "$set": {
                    "acknowledged": True,
                },
            },
            upsert=True,
        )
