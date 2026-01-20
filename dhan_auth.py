# dhan_auth.py
import os, time, requests

class DhanAuth:
    def __init__(self):
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.api_secret = os.getenv("DHAN_API_SECRET")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN")
        self.refresh_token = os.getenv("DHAN_REFRESH_TOKEN")
        self.base_url = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")
        self.expires_at = time.time() + 3600  # assume valid for 1 hour initially

    def get_token(self):
        """Return valid access token, refresh if expired"""
        if time.time() < self.expires_at:
            return self.access_token

        if not self.refresh_token:
            print("⚠️ No refresh token found — using static token.")
            return self.access_token

        try:
            print("🔁 Refreshing Dhan token...")
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
            print("✅ Token refreshed successfully.")
        except Exception as e:
            print("❌ Failed to refresh token:", e)
        return self.access_token
