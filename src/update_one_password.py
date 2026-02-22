"""
update_tableau_password_TEST.py
================================
This is a SINGLE-ITEM TEST version of the full password update script.

Use this first to verify everything is working correctly before running
the full script across all datasources and workbooks.

HOW TO USE:
  1. Fill in the CONFIG section below.
  2. Set TEST_ITEM_NAME to the exact name of one datasource or workbook
     as it appears in Tableau (capitalization matters).
  3. Set TEST_ITEM_TYPE to either "datasource" or "workbook".
  4. Run the script:  python update_tableau_password_TEST.py
  5. Log into Tableau and confirm the connection works for that one item.
  6. If it looks good, run the full script: update_tableau_password.py

REQUIRED .env FILE KEYS:
  ts_token          - Your Tableau Personal Access Token name
  ts_secret         - Your Tableau Personal Access Token secret
  api_version       - Tableau REST API version (e.g. "3.19")
  server_url        - Your Tableau Server base URL (e.g. "https://tableau.yourdomain.com")
  sf_username       - The Snowflake username whose password needs updating (e.g. NAS230@PITT.EDU)
  sf_password       - The new Snowflake password to embed
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
TEST_ITEM_NAME = "MyFunding Budget"

# What type of item is it? Must be either "datasource" or "workbook".
TEST_ITEM_TYPE = "datasource"

# ─────────────────────────────────────────────
#  END OF CONFIG — no changes needed below
# ─────────────────────────────────────────────


def load_env_or_exit():
    """Load environment variables and exit with a clear error if any are missing."""
    load_dotenv()
    required_keys = ["ts_token", "ts_secret", "api_version", "server_url", "sf_username", "sf_password"]
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

    return config


def authenticate(config):
    """Log in to the Tableau REST API and return an auth header + site URL."""
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
        print(f"  - Make sure the name matches exactly as it appears in Tableau (including capitalization).")
        print(f"  - Make sure TEST_ITEM_TYPE is set to the correct type ('datasource' or 'workbook').")
        sys.exit(1)

    if len(matches) > 1:
        print(f"\nWARNING: Found {len(matches)} {dtype}s with the name '{name}'.")
        print("  Tableau allows duplicate names in different projects. Showing all matches:\n")
        for i, m in enumerate(matches, 1):
            project = m.get("project", {}).get("name", "—")
            owner = m.get("owner", {}).get("name", "—")
            print(f"  {i}. Name: {m['name']}  |  Project: {project}  |  Owner: {owner}  |  ID: {m['id']}")
        print("\nThis test script will use the first match. If that's not the right one,")
        print("consider using the full script and filtering by owner or project instead.")

    return matches[0]


def find_matching_connections(item, dtype, username, site_url, headers):
    """
    For a given item, return all Snowflake connections that match the target username.
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
        if (cx.get("userName", "").lower() == username.lower()
                and cx.get("type") == "snowflake"):
            record = {
                "connection_id": cx.get("id"),
                "userName": cx.get("userName"),
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


def update_password(record, new_password, site_url, headers):
    """Update the embedded password for a single connection."""
    endpoint = "workbooks" if record["dtype"] == "workbook" else "datasources"
    url = f"{site_url}/{endpoint}/{record['item_id']}/connections/{record['connection_id']}"

    payload = {
        "connection": {
            "userName": record["userName"].lower(),
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
        print("Open this file and replace 'your-datasource-or-workbook-name-here' with the actual name.")
        sys.exit(1)

    if TEST_ITEM_TYPE not in ("datasource", "workbook"):
        print(f"ERROR: TEST_ITEM_TYPE must be 'datasource' or 'workbook', but got '{TEST_ITEM_TYPE}'.")
        sys.exit(1)

    # Step 1 — Load credentials from .env
    config = load_env_or_exit()
    user_to_update = config["sf_username"]
    new_password = config["sf_password"]

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
        print(f"  This item is owned by '{item_owner}', but you are logged in as '{token_username}'.")
        print(f"  Only the owner can update embedded connection credentials.")
        print(f"  Try a different item that you own, or ask '{item_owner}' to run the script.")
        sys.exit(1)

    # Step 4 — Find matching Snowflake connections on that item
    print(f"Looking for Snowflake connections using username: {user_to_update}")
    connections = find_matching_connections(item, TEST_ITEM_TYPE, user_to_update, site_url, headers)

    if not connections:
        print(f"\nNo matching Snowflake connections found for user '{user_to_update}' on this item.")
        print("Things to check:")
        print("  - Is sf_username in your .env file spelled correctly (including the domain, e.g. @PITT.EDU)?")
        print("  - Does this item actually connect to Snowflake?")
        print("  - Is the connection embedded, or does it use a published datasource?")
        sys.exit(0)

    print(f"Found {len(connections)} matching connection(s).\n")

    # Step 5 — Ask the user to confirm before making any changes.
    # This gives the user a chance to double-check the details above
    # before anything is actually updated in Tableau.
    print(f"You are about to update the embedded Snowflake password for '{user_to_update}'")
    print(f"on the {TEST_ITEM_TYPE}: '{TEST_ITEM_NAME}'.\n")
    confirm = input("Type YES to proceed, or anything else to cancel: ").strip()

    if confirm != "YES":
        print("\nCancelled. No changes were made.")
        sys.exit(0)

    print()

    # Step 6 — Update the password
    print("Updating password...\n")
    success_count = 0
    fail_count = 0

    for cx in connections:
        print(f"  Updating connection on '{cx['item_name']}' ({cx['dtype']})...", end=" ")
        if update_password(cx, new_password, site_url, headers):
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