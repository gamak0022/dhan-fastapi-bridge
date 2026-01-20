import os, time, requests, json

TOKEN_FILE = "token_store.json"

class DhanAuth:
    def __init__(self):
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.api_secret = os.getenv("DHAN_API_SECRET")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN")
        self.refresh_token = os.getenv("DHAN_REFRESH_TOKEN")
        self.base_url = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")
        self.expires_at = time.time() + 3600
        self._load_token()

    def _load_token(self):
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                self.access_token = data.get("access_token", self.access_token)
                self.expires_at = data.get("expires_at", self.expires_at)

    def _save_token(self):
        with open(TOKEN_FILE, "w") as f:
            json.dump({
                "access_token": self.access_token,
                "expires_at": self.expires_at
            }, f)

    def get_token(self):
        if time.time() < self.expires_at:
            return self.access_token
        if not self.refresh_token:
            return self.access_token
        try:
            r = requests.post(
                f"{self.base_url}/token/refresh",
                json={
                    "client_id": self.client_id,
                    "client_secret": self.api_secret,
                    "refresh_token": self.refresh_token
                },
            )
            r.raise_for_status()
            data = r.json()
            self.access_token = data.get("access_token", self.access_token)
            self.expires_at = time.time() + int(data.get("expires_in", 3600))
            self._save_token()
            print("✅ Dhan token refreshed.")
        except Exception as e:
            print("⚠️ Refresh failed:", e)
        return self.access_token
