<<<<<<< HEAD
# Telegram Join Verification Bot

Production-grade Telegram bot that approves join requests only after language selection and human verification.

## Features
- Join request intake with per-chat verification tokens
- Language selection (English/हिंदी/Hinglish)
- Human verification with randomized buttons
- Max attempts + timeout enforcement
- Admin controls for settings and lists

## Requirements
- Python 3.11+
- Telegram bot token with admin rights in the target channels/groups

## Quick Start
1) Create and activate a virtual environment
2) Install dependencies:
   - `pip install -r requirements.txt`
3) Set environment variables:
   - `BOT_TOKEN` (required)
   - `ADMIN_IDS` (comma-separated Telegram user IDs)
   - `DB_PATH` (default: `bot.db`)
   - `MAX_ATTEMPTS` (default: `3`)
   - `VERIFY_TIMEOUT_SECONDS` (default: `120`)
   - `LANG_TIMEOUT_SECONDS` (default: `120`)
   - `FAILURE_ACTION` (default: `reject`, options: `reject` or `pending`)
4) Run:
   - `python main.py`

## Admin Commands
- `/status`
- `/setattempts <number>`
- `/settimeout <seconds>`
- `/approve <user_id> [chat_id]` (whitelisted users only)
- `/reject <user_id> [chat_id]`
- `/whitelist add <user_id>`
- `/blacklist add <user_id>`

## Notes
- If the bot cannot DM a user, it will not approve the join request.
- Manual `/approve` requires the user to be whitelisted, acting as explicit admin verification.
=======
# AUTO-APPROVAL-BOT
Production-grade Telegram bot that approves join requests only after language selection and human verification.

