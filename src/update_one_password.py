"""
update_tableau_password_TEST.py
================================
This is a SINGLE-ITEM TEST version of the full password update script.

Use this first to verify everything is working correctly before running
the full script across all datasources and workbooks.

Like the full script, it supports two modes controlled by your .env file:

  PASSWORD ROTATION (most common)
    Leave sf_old_username blank or omit it from your .env file.

  USERNAME + PASSWORD MIGRATION
    Set sf_old_username to the current (old) username in your .env file.

HOW TO USE:
  1. Make sure your .env file is set up (see the required keys listed below).
  2. Set TEST_ITEM_NAME to the exact name of one datasource or workbook
     as it appears in Tableau (capitalization matters).
  3. Set TEST_ITEM_TYPE to either "datasource" or "workbook".
  4. Run the script:  python update_one_password.py
  5. Log into Tableau and confirm the connection works for that one item.
  6. If you want to manually confirm each one, you can type in each additional 
     data source; or, to run the update for all data connections that you own, 
     run the full script: update_tableau_password.py

REQUIRED .env FILE KEYS:
  ts_token          - Your Tableau Personal Access Token name
  ts_secret         - Your Tableau Personal Access Token secret
  api_version       - Tableau REST API version (e.g. "3.19")
  server_url        - Your Tableau Server base URL (e.g. "https://tableau.yourdomain.com")
  sf_username       - The Snowflake username whose password needs updating (e.g. NAS230@PITT.EDU)
  sf_password       - The new Snowflake password to embed

OPTIONAL .env FILE KEYS:
  sf_old_username   - The current Snowflake username to search for (migration mode only).
                      If blank or omitted, the script runs in password rotation mode
                      and searches for connections already using sf_username.
"""

import requests
import json
import os
import sys
from dotenv import load_dotenv

# ─────────────────────────────────────────────
#  CONFIG — only edit values in this section
# ─────────────────────────────────────────────

# The exact name of the datasource or workbook you want to test on.
# This must match the name as it appears in Tableau exactly.
# Example: "Enrollment Dashboard" or "Student Finance Source"
TEST_ITEM_NAME = "your-datasource-or-workbook-name-here"

# What type of item is it? Must be either "datasource" or "workbook".
TEST_ITEM_TYPE = "datasource"

# ─────────────────────────────────────────────
#  END OF CONFIG — no changes needed below
# ─────────────────────────────────────────────


def load_env_or_exit():
    """
    Load environment variables from .env and exit with a clear error if any
    required keys are missing. Also reads the optional sf_old_username key.
    """
    load_dotenv()
    required_keys = ["ts_token", "ts_secret", "api_version", "server_url",
                     "sf_username", "sf_password"]
    config = {}
    missing = []

    for key in required_keys:
        val = os.getenv(key)
        if not val:
            missing.append(key)
        else:
            config[key] = val

    if missing:
        print("ERROR: The following required keys are missing from your .env file:")
        for k in missing:
            print(f"  - {k}")
        print("\nPlease add them to your .env file and try again.")
        sys.exit(1)

    # sf_old_username is optional — if present and non-empty, migration mode is active
    config["sf_old_username"] = os.getenv("sf_old_username", "").strip()

    return config


def authenticate(config):
    """
    Log in to the Tableau REST API using a Personal Access Token.
    Returns the auth header, site-level base URL, and the token owner's username.
    The username is fetched via a second API call since the sign-in response
    only returns the user's ID.
    """
    url = f"{config['server_url']}/api/{config['api_version']}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": config["ts_token"],
            "personalAccessTokenSecret": config["ts_secret"],
            "site": {"contentUrl": "u"}
        }
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    print("Authenticating with Tableau Server...")
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: Authentication failed — {e}")
        print("Check that your ts_token, ts_secret, and server_url are correct.")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Could not reach the server at {config['server_url']}.")
        print("Check your server_url and network connection.")
        sys.exit(1)

    creds = r.json()["credentials"]
    site_id = creds["site"]["id"]
    user_id = creds["user"]["id"]
    token = creds["token"]

    auth_header = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Tableau-Auth": token
    }
    site_url = f"{config['server_url']}/api/{config['api_version']}/sites/{site_id}"

    # The sign-in response only returns the user's ID, not their name.
    # We make a second call to look up the full user record so we have
    # the exact username as Tableau stores it — this is used to match
    # asset ownership and must be letter-perfect to work correctly.
    try:
        user_r = requests.get(f"{site_url}/users/{user_id}", headers=auth_header)
        user_r.raise_for_status()
        token_username = user_r.json()["user"]["name"]
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: Authenticated successfully but could not look up user details — {e}")
        sys.exit(1)

    print(f"Authentication successful. Logged in as: {token_username}\n")
    return auth_header, site_url, token_username


def get_all_pages(base_url, headers, page_size=100):
    """
    Fetches all pages of results from a Tableau API list endpoint.
    Returns a flat list of all items.
    """
    items = []
    page = 0
    total_available = 1

    while page * page_size < total_available:
        page += 1
        url = f"{base_url}?pageNumber={page}&pageSize={page_size}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        total_available = int(data["pagination"]["totalAvailable"])
        # The inner key is either "datasource" or "workbook" (singular)
        inner_key = list(data[[k for k in data.keys() if k != "pagination"][0]].keys())[0]
        outer_key = [k for k in data.keys() if k != "pagination"][0]
        items += data[outer_key][inner_key]

    return items


def find_item_by_name(name, dtype, site_url, headers):
    """
    Search for a datasource or workbook by its exact name.
    Returns the matching item dict, or None if not found.

    Think of this like searching a filing cabinet by label —
    if the label doesn't match exactly, the folder won't be found.
    """
    endpoint = "workbooks" if dtype == "workbook" else "datasources"
    print(f"Searching for {dtype} named '{name}'...")

    try:
        all_items = get_all_pages(f"{site_url}/{endpoint}", headers)
    except Exception as e:
        print(f"ERROR fetching {dtype}s: {e}")
        sys.exit(1)

    matches = [i for i in all_items if i.get("name") == name]

    if not matches:
        print(f"\nERROR: No {dtype} found with the name '{name}'.")
        print("  - Make sure the name matches exactly as it appears in Tableau "
              "(including capitalization).")
        print("  - Make sure TEST_ITEM_TYPE is set to the correct type "
              "('datasource' or 'workbook').")
        sys.exit(1)

    if len(matches) > 1:
        print(f"\nWARNING: Found {len(matches)} {dtype}s with the name '{name}'.")
        print("  Tableau allows duplicate names in different projects. "
              "Showing all matches:\n")
        for i, m in enumerate(matches, 1):
            project = m.get("project", {}).get("name", "—")
            owner = m.get("owner", {}).get("name", "—")
            print(f"  {i}. Name: {m['name']}  |  Project: {project}  "
                  f"|  Owner: {owner}  |  ID: {m['id']}")
        print("\nThis test script will use the first match. If that's not the right one,")
        print("consider specifying a different item name.")

    return matches[0]


def find_matching_connections(item, dtype, target_username, site_url, headers):
    """
    For a given item, return all Snowflake connections whose current username
    matches target_username.
    """
    item_id = item.get("id")
    endpoint = "workbooks" if dtype == "workbook" else "datasources"
    url = f"{site_url}/{endpoint}/{item_id}/connections/"

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: Could not fetch connections for '{item.get('name')}' — {e}")
        sys.exit(1)

    connections = r.json().get("connections", {}).get("connection")
    if not connections:
        return []

    matched = []
    for cx in connections:
        if (cx.get("userName", "").lower() == target_username.lower()
                and cx.get("type") == "snowflake"):
            record = {
                "connection_id": cx.get("id"),
                "current_username": cx.get("userName"),
                "item_id": item_id,
                "item_name": item.get("name"),
                "dtype": dtype,
                "owner": item.get("owner", {}).get("name"),
            }
            if dtype == "workbook":
                record["url"] = item.get("webpageUrl")
            else:
                record["project"] = item.get("project", {}).get("name")
            matched.append(record)

    return matched


def update_connection(record, new_username, new_password, site_url, headers):
    """
    Send updated credentials to Tableau for a single connection.
    Accepts both a new username and new password, supporting both
    password rotation (same username) and migration (new username).
    Returns True if successful, False otherwise.
    """
    endpoint = "workbooks" if record["dtype"] == "workbook" else "datasources"
    url = (f"{site_url}/{endpoint}/{record['item_id']}"
           f"/connections/{record['connection_id']}")

    payload = {
        "connection": {
            "userName": new_username.lower(),
            "password": new_password,
            "embedPassword": "true"
        }
    }

    try:
        r = requests.put(url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        print(f"  ERROR: {e}")
        return False


def main():
    # Validate config values before doing anything
    if TEST_ITEM_NAME == "your-datasource-or-workbook-name-here":
        print("ERROR: You haven't set TEST_ITEM_NAME in the CONFIG section yet.")
        print("Open this file and replace the placeholder with the actual item name.")
        sys.exit(1)

    if TEST_ITEM_TYPE not in ("datasource", "workbook"):
        print(f"ERROR: TEST_ITEM_TYPE must be 'datasource' or 'workbook', "
              f"but got '{TEST_ITEM_TYPE}'.")
        sys.exit(1)

    # Step 1 — Load credentials from .env
    config = load_env_or_exit()

    new_username = config["sf_username"]
    new_password = config["sf_password"]
    old_username = config["sf_old_username"]

    # Determine mode based on whether sf_old_username is set.
    if old_username:
        mode = "migration"
        target_username = old_username
        print("Mode: USERNAME + PASSWORD MIGRATION")
        print(f"  Searching for connections using: {old_username}")
        print(f"  Will update to username: {new_username} with new password\n")
    else:
        mode = "rotation"
        target_username = new_username
        print("Mode: PASSWORD ROTATION")
        print(f"  Searching for connections using: {new_username}")
        print("  Will update password only (username unchanged)\n")

    # Step 2 — Authenticate
    headers, site_url, token_username = authenticate(config)

    # Step 3 — Find the named item
    item = find_item_by_name(TEST_ITEM_NAME, TEST_ITEM_TYPE, site_url, headers)
    item_owner = item.get("owner", {}).get("name", "")
    print(f"Found: '{item.get('name')}'  |  Owner: {item_owner}  |  ID: {item.get('id')}\n")

    # Check that the token user owns this item before attempting any update.
    # Trying to update something you don't own will result in a permissions error,
    # so we catch this early and give a clear explanation.
    if item_owner.lower() != token_username.lower():
        print(f"ERROR: You do not have permission to update this {TEST_ITEM_TYPE}.")
        print(f"  This item is owned by '{item_owner}', but you are logged in "
              f"as '{token_username}'.")
        print(f"  Only the owner can update embedded connection credentials.")
        print(f"  Try a different item that you own, or ask '{item_owner}' "
              f"to run the script.")
        sys.exit(1)

    # Step 4 — Find matching Snowflake connections on that item
    print(f"Looking for Snowflake connections using username: {target_username}")
    connections = find_matching_connections(item, TEST_ITEM_TYPE, target_username, 
                                            site_url, headers)

    if not connections:
        key = "sf_old_username" if mode == "migration" else "sf_username"
        print(f"\nNo matching Snowflake connections found for user '{target_username}' "
              f"on this item.")
        print("Things to check:")
        print(f"  - Is {key} in your .env file spelled correctly "
              "(including the domain, e.g. @PITT.EDU)?")
        print("  - Does this item actually connect to Snowflake?")
        print("  - Is the connection embedded, or does it use a published datasource?")
        sys.exit(0)

    print(f"Found {len(connections)} matching connection(s).\n")

    # Step 5 — Ask the user to confirm before making any changes.
    if mode == "migration":
        print(f"You are about to update the Snowflake username from '{old_username}' "
              f"to '{new_username}'")
    else:
        print(f"You are about to update the embedded Snowflake password for "
              f"'{new_username}'")
    print(f"on the {TEST_ITEM_TYPE}: '{TEST_ITEM_NAME}'.\n")
    confirm = input("Type YES to proceed, or anything else to cancel: ").strip()

    if confirm != "YES":
        print("\nCancelled. No changes were made.")
        sys.exit(0)

    print()

    # Step 6 — Update the password
    action = "credentials" if mode == "migration" else "password"
    print(f"Updating {action}...\n")
    success_count = 0
    fail_count = 0

    for cx in connections:
        print(f"  Updating connection on '{cx['item_name']}' ({cx['dtype']})...",
              end=" ")
        if update_connection(cx, new_username, new_password, site_url, headers):
            print("OK")
            success_count += 1
        else:
            fail_count += 1

    # Step 7 — Summary and next steps
    print(f"\n{'─'*50}")
    if success_count:
        print(f"✓ {success_count} connection(s) updated successfully.")
        print(f"\nNEXT STEP: Log into Tableau and open '{TEST_ITEM_NAME}' to confirm")
        print("the data loads correctly.")
    if fail_count:
        print(f"✗ {fail_count} connection(s) failed — see errors above.")
    print(f"{'─'*50}")


if __name__ == "__main__":
    main()