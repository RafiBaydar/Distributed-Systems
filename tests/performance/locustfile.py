from locust import HttpUser, task, between
import requests, random, time, os

# Node list untuk deteksi leader
NODES = os.getenv("DQ_NODES", "http://localhost:8080,http://localhost:8081,http://localhost:8082").split(",")

_leader = None
def get_leader() -> str:
    """Deteksi leader; cache sementara. Jika gagal, fallback ke cache lama."""
    global _leader
    for url in NODES:
        try:
            r = requests.get(f"{url}/raft/health", timeout=1.0)
            j = r.json()
            if j.get("state") == "leader":
                _leader = url
                return _leader
        except Exception:
            pass
    return _leader or NODES[0]

def _retry_to_leader(client, path: str, json_body: dict, name: str):
    """Kirim POST ke leader; jika 409 not_leader, redirect ke hint leader."""
    url = f"{get_leader()}{path}"
    with client.post(url, json=json_body, name=name, catch_response=True) as resp:
        if resp.status_code == 409:
            try:
                hint = resp.json().get("leader")  # format: "nodeX:PORT"
                if hint:
                    url2 = f"http://{hint}{path}"
                    r2 = client.post(url2, json=json_body, name=name)
                    if r2.status_code < 400:
                        resp.success()
                    else:
                        resp.failure(r2.text)
                else:
                    resp.failure("409 not_leader (no hint)")
            except Exception as e:
                resp.failure(str(e))
        elif resp.status_code >= 400:
            resp.failure(resp.text)
        else:
            resp.success()

class DistUser(HttpUser):
    wait_time = between(0.01, 0.05)  # kecil biar RPS naik

    @task(4)
    def queue_flow(self):
        topic = "bench"
        payload = {"i": random.randint(1, 1_000_000), "ts": int(time.time() * 1000)}
        base = get_leader()

        # produce
        self.client.post(f"{base}/queue/produce",
                         json={"topic": topic, "payload": payload},
                         name="/queue/produce")

        # consume
        r = self.client.get(f"{base}/queue/consume?topic={topic}&max=1&visibility_ttl=15",
                            name="/queue/consume")
        if r.ok:
            msgs = r.json().get("messages") or []
            if msgs:
                mid = msgs[0].get("msg_id") or msgs[0].get("id") or msgs[0].get("message_id")
                if mid:
                    # ack
                    self.client.post(f"{base}/queue/ack",
                                     json={"topic": topic, "msg_id": mid},
                                     name="/queue/ack")

    @task(1)
    def lock_flow(self):
        # random resource & owner
        res = f"res-{random.randint(1, 500)}"
        owner = f"cli-{random.randint(1, 500)}"
        # acquire (auto-follow leader jika perlu)
        _retry_to_leader(self.client, "/lock/acquire",
                         {"resource": res, "owner": owner, "mode": "x"},
                         name="/lock/acquire")
        # release
        _retry_to_leader(self.client, "/lock/release",
                         {"resource": res, "owner": owner},
                         name="/lock/release")
