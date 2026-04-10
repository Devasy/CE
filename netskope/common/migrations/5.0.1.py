"""Migrations for 5.0.1 release."""
import traceback
from netskope.common.utils import DBConnector, Collections, RepoManager
from netskope.common.models import PluginRepo


connector = DBConnector()
manager = RepoManager()


def reset_default_git_repo():
    """Reset the default git repo to fix permissions of files."""
    print("Resetting Default repo head")
    try:
        connector = DBConnector()
        repo = connector.collection(Collections.PLUGIN_REPOS).find_one({"name": "Default"})
        repo = PluginRepo(**repo)
        result = manager.reset_hard_to_head(repo)
        if isinstance(result, str):
            print(result)
    except Exception:
        print("Error occurred while resetting the head", traceback.format_exc())


def remove_security_scorecard_banner():
    """Remove SecurityScorecard banner."""
    connector.collection(Collections.NOTIFICATIONS).delete_one(
        {"id": "BANNER_INFO_1001"}
    )


if __name__ == "__main__":
    reset_default_git_repo()
    remove_security_scorecard_banner()
