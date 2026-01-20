import os, time, json, requests

TOKEN_FILE = "token_store.json"

class DhanAuth:
    def __init__(self):
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.api_secret = os.getenv("DHAN_API_SECRET")
        self.refresh_token = os.getenv("DHAN_REFRESH_TOKEN")
        self.base_url = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")

        self.access_token = None
        self.expires_at = 0
        self._load_token()

    def _load_token(self):
        """Load saved token from JSON file"""
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                self.access_token = data.get("access_token")
                self.expires_at = data.get("expires_at", 0)

    def _save_token(self):
        """Save token with expiry time"""
        with open(TOKEN_FILE, "w") as f:
            json.dump({
                "access_token": self.access_token,
                "expires_at": self.expires_at
            }, f)

    def get_token(self):
        """Return a valid token, refresh if expired"""
        if self.access_token and time.time() < self.expires_at:
            return self.access_token
        return self._refresh_token()

    def _refresh_token(self):
        """Refresh Dhan token via API"""
        try:
            print("🔁 Refreshing Dhan token...")
            r = requests.post(
                f"{self.base_url}/token/refresh",
                json={
                    "client_id": self.client_id,
                    "client_secret": self.api_secret,
                    "refresh_token": self.refresh_token
                },
                timeout=10
            )
            r.raise_for_status()
            data = r.json()
            self.access_token = data["access_token"]
            ttl = int(data.get("expires_in", 86400))
            self.expires_at = time.time() + ttl
            self._save_token()
            print("✅ Token refreshed; valid for", round(ttl/3600, 2), "hours.")
        except Exception as e:
            print("❌ Token refresh failed:", e)
            if not self.access_token:
                raise RuntimeError("No valid Dhan access token!")
        return self.access_token
