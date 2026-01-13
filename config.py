import os


class Config:
    def __init__(self) -> None:
        self.bot_token = os.environ.get("BOT_TOKEN", "").strip()
        self.db_path = os.environ.get("DB_PATH", "bot.db").strip()
        self.admin_ids = self._parse_admin_ids(os.environ.get("ADMIN_IDS", ""))
        self.max_attempts = int(os.environ.get("MAX_ATTEMPTS", "3"))
        self.verification_timeout_seconds = int(
            os.environ.get("VERIFY_TIMEOUT_SECONDS", "120")
        )
        self.language_timeout_seconds = int(
            os.environ.get("LANG_TIMEOUT_SECONDS", "120")
        )
        self.failure_action = os.environ.get("FAILURE_ACTION", "reject").strip().lower()
        self.log_level = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
        if self.failure_action not in {"reject", "pending"}:
            self.failure_action = "reject"

    @staticmethod
    def _parse_admin_ids(value: str) -> set[int]:
        ids: set[int] = set()
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except ValueError:
                continue
        return ids
