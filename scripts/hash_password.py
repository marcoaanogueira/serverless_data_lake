#!/usr/bin/env python3
"""
Generate login credentials for the Data Lake frontend auth.

Usage:
    python scripts/hash_password.py

Then copy the AWS CLI command printed at the end and run it to store
the credentials in Secrets Manager.
"""

import getpass
import hashlib
import json
import os


def main():
    print("=== Data Lake — Set Auth Credentials ===\n")
    email = input("Email: ").strip()
    if not email:
        print("Error: email cannot be empty.")
        return

    password = getpass.getpass("Password: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: passwords do not match.")
        return

    salt = os.urandom(32).hex()
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 260_000)
    password_hash = key.hex()

    secret_value = json.dumps({
        "email": email,
        "password_hash": password_hash,
        "salt": salt,
    })

    print("\n--- Copy and run this AWS CLI command ---\n")
    print(
        f"aws secretsmanager put-secret-value \\\n"
        f"    --secret-id /data-lake/auth-credentials \\\n"
        f"    --secret-string '{secret_value}'"
    )
    print("\nDone. Deploy or redeploy the stack and your login page will be active.")


if __name__ == "__main__":
    main()
