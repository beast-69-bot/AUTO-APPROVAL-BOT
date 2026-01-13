import aiosqlite
from typing import Any, Optional


class Database:
    def __init__(self, path: str) -> None:
        self.path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS join_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    language TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    language_token TEXT,
                    language_expires_at INTEGER,
                    verification_token TEXT,
                    verification_expires_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_join_req_user_chat "
                "ON join_requests(user_id, chat_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_join_req_lang_token "
                "ON join_requests(language_token)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_join_req_ver_token "
                "ON join_requests(verification_token)"
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS whitelist (
                    user_id INTEGER PRIMARY KEY,
                    created_at INTEGER NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id INTEGER PRIMARY KEY,
                    created_at INTEGER NOT NULL
                )
                """
            )
            await db.commit()

    async def get_setting(self, key: str, default: str) -> str:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cur.fetchone()
            if not row:
                return default
            return row[0]

    async def set_setting(self, key: str, value: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            await db.commit()

    async def is_whitelisted(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT 1 FROM whitelist WHERE user_id = ?", (user_id,)
            )
            return await cur.fetchone() is not None

    async def is_blacklisted(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT 1 FROM blacklist WHERE user_id = ?", (user_id,)
            )
            return await cur.fetchone() is not None

    async def add_whitelist(self, user_id: int, now: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO whitelist(user_id, created_at) VALUES(?, ?)",
                (user_id, now),
            )
            await db.commit()

    async def add_blacklist(self, user_id: int, now: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO blacklist(user_id, created_at) VALUES(?, ?)",
                (user_id, now),
            )
            await db.commit()

    async def upsert_join_request(
        self,
        user_id: int,
        chat_id: int,
        status: str,
        now: int,
        language_token: str,
        language_expires_at: int,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO join_requests(
                    user_id, chat_id, status, language_token, language_expires_at,
                    created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    chat_id,
                    status,
                    language_token,
                    language_expires_at,
                    now,
                    now,
                ),
            )
            await db.commit()

    async def get_join_request_by_lang_token(self, token: str) -> Optional[dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id, user_id, chat_id, status, language, attempts,
                       language_expires_at, verification_expires_at
                FROM join_requests
                WHERE language_token = ?
                """,
                (token,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "user_id": row[1],
                "chat_id": row[2],
                "status": row[3],
                "language": row[4],
                "attempts": row[5],
                "language_expires_at": row[6],
                "verification_expires_at": row[7],
            }

    async def get_join_request_by_ver_token(self, token: str) -> Optional[dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id, user_id, chat_id, status, language, attempts,
                       verification_expires_at
                FROM join_requests
                WHERE verification_token = ?
                """,
                (token,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "user_id": row[1],
                "chat_id": row[2],
                "status": row[3],
                "language": row[4],
                "attempts": row[5],
                "verification_expires_at": row[6],
            }

    async def set_language_and_verification(
        self,
        request_id: int,
        language: str,
        verification_token: str,
        verification_expires_at: int,
        now: int,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET language = ?, status = ?, verification_token = ?,
                    verification_expires_at = ?, language_token = NULL,
                    language_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (
                    language,
                    "awaiting_verification",
                    verification_token,
                    verification_expires_at,
                    now,
                    request_id,
                ),
            )
            await db.commit()

    async def increment_attempts(self, request_id: int, now: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET attempts = attempts + 1, updated_at = ?
                WHERE id = ?
                """,
                (now, request_id),
            )
            await db.commit()

    async def mark_verified(self, request_id: int, now: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET status = ?, verification_token = NULL, verification_expires_at = NULL,
                    language_token = NULL, language_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                ("verified", now, request_id),
            )
            await db.commit()

    async def mark_failed(self, request_id: int, now: int, status: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET status = ?, verification_token = NULL, verification_expires_at = NULL,
                    language_token = NULL, language_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (status, now, request_id),
            )
            await db.commit()

    async def list_expired_language(self, now: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id, user_id, chat_id
                FROM join_requests
                WHERE status = ? AND language_expires_at IS NOT NULL
                  AND language_expires_at <= ?
                """,
                ("awaiting_language", now),
            )
            rows = await cur.fetchall()
            return [{"id": r[0], "user_id": r[1], "chat_id": r[2]} for r in rows]

    async def list_expired_verification(self, now: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id, user_id, chat_id, language
                FROM join_requests
                WHERE status = ? AND verification_expires_at IS NOT NULL
                  AND verification_expires_at <= ?
                """,
                ("awaiting_verification", now),
            )
            rows = await cur.fetchall()
            return [
                {"id": r[0], "user_id": r[1], "chat_id": r[2], "language": r[3]}
                for r in rows
            ]

    async def count_statuses(self, chat_id: Optional[int]) -> dict[str, int]:
        async with aiosqlite.connect(self.path) as db:
            if chat_id is None:
                cur = await db.execute(
                    "SELECT status, COUNT(1) FROM join_requests GROUP BY status"
                )
            else:
                cur = await db.execute(
                    "SELECT status, COUNT(1) FROM join_requests "
                    "WHERE chat_id = ? GROUP BY status",
                    (chat_id,),
                )
            rows = await cur.fetchall()
            return {row[0]: row[1] for row in rows}

    async def get_latest_request_id(self, user_id: int, chat_id: int) -> int:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id FROM join_requests
                WHERE user_id = ? AND chat_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (user_id, chat_id),
            )
            row = await cur.fetchone()
            return row[0] if row else 0

    async def mark_status_for_user_chat(
        self, user_id: int, chat_id: int, status: str, now: int
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET status = ?, updated_at = ?
                WHERE id = (
                    SELECT id FROM join_requests
                    WHERE user_id = ? AND chat_id = ?
                    ORDER BY id DESC LIMIT 1
                )
                """,
                (status, now, user_id, chat_id),
            )
            await db.commit()

    async def get_pending_requests_for_user(self, user_id: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """
                SELECT id, chat_id, status, language, language_token, language_expires_at,
                       verification_token, verification_expires_at
                FROM join_requests
                WHERE user_id = ? AND status IN (?, ?)
                ORDER BY updated_at DESC
                """,
                (user_id, "awaiting_language", "awaiting_verification"),
            )
            rows = await cur.fetchall()
            return [
                {
                    "id": r[0],
                    "chat_id": r[1],
                    "status": r[2],
                    "language": r[3],
                    "language_token": r[4],
                    "language_expires_at": r[5],
                    "verification_token": r[6],
                    "verification_expires_at": r[7],
                }
                for r in rows
            ]

    async def update_language_token(
        self, request_id: int, token: str, expires_at: int, now: int
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET language_token = ?, language_expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (token, expires_at, now, request_id),
            )
            await db.commit()

    async def update_verification_token(
        self, request_id: int, token: str, expires_at: int, now: int
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE join_requests
                SET verification_token = ?, verification_expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (token, expires_at, now, request_id),
            )
            await db.commit()
