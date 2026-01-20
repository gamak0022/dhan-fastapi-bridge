import os, time, json, requests

TOKEN_FILE = "token_store.json"

class DhanAuth:
    def __init__(self):
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.api_secret = os.getenv("DHAN_API_SECRET")
        self.base_url = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")
        self.access_token = None
        self.expires_at = 0
        self._load_token()

    def _load_token(self):
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                self.access_token = data.get("access_token")
                self.expires_at = data.get("expires_at", 0)

    def _save_token(self):
        with open(TOKEN_FILE, "w") as f:
            json.dump({
                "access_token": self.access_token,
                "expires_at": self.expires_at
            }, f)

    def get_token(self):
        if self.access_token and time.time() < self.expires_at:
            return self.access_token
        return self._login_for_new_token()

    def _login_for_new_token(self):
        """Re-authenticate using client_id + api_secret"""
        try:
            print("🔑 Requesting new Dhan access token ...")
            r = requests.post(
                f"{self.base_url}/login",
                json={
                    "client_id": self.client_id,
                    "client_secret": self.api_secret
                },
                timeout=10
            )
            r.raise_for_status()
            data = r.json()
            self.access_token = data["access_token"]
            self.expires_at = time.time() + 23 * 3600   # assume 23 h validity
            self._save_token()
            print("✅ Token refreshed successfully.")
        except Exception as e:
            print("❌ Login failed:", e)
            if not self.access_token:
                raise RuntimeError("No valid Dhan access token!")
        return self.access_token
