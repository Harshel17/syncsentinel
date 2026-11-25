from fastapi.testclient import TestClient

from syncsentinel.simulator import create_simulated_stacksync


def test_simulator_lists_and_resolves_documented_issue_contract(sync_issue):
    client = TestClient(create_simulated_stacksync([sync_issue]))
    path = "/v1/workspaces/42/issues"

    listed = client.get(path, params={"region": "usnv", "base_id": sync_issue.base_id})
    assert listed.status_code == 200
    assert listed.json()["data"][0]["issue_id"] == sync_issue.issue_id

    resolved = client.post(
        f"{path}/action",
        params={"region": "usnv"},
        json={"ids": [sync_issue.issue_id], "action": "retry"},
    )
    assert resolved.status_code == 200
    assert resolved.json()["data"]["success_issue_count"] == 1
    assert client.get(path, params={"region": "usnv"}).json()["data"] == []
