from locust import HttpUser, task, between
class LockUser(HttpUser):
    wait_time = between(0.1, 0.5)
    @task
    def flow(self):
        self.client.post('/lock/acquire', json={'resource':'r1','owner':'u1','mode':'x'})
        self.client.post('/lock/release', json={'resource':'r1','owner':'u1'})
