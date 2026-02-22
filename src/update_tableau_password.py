"""
update_tableau_password.py
==========================
This script automatically updates the embedded Snowflake crednetials across all 
Tableau datasources and workbooks that you own on your Tableau Server.

It supports two modes, controlled entirely by your .env file:

  PASSWORD ROTATION (most common)
    Update the password on connections already using sf_username.
    Leave sf_old_username blank or omit it from your .env file.

  USERNAME + PASSWORD MIGRATION
    Rename a Snowflake account and update its password at the same time.
    Set sf_old_username to the current (old) username in your .env file.
    The script will find all connections using that old username and update
    them to use sf_username and sf_password.

HOW TO USE:
  1. Make sure your .env file is set up (see the required keys listed below).
  2. Run the script:  python update_tableau_password.py

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


def load_env_or_exit():
    """
    Load environment variables and exit with a clear error if any are missing.
    """

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

def get_all_pages(base_url, key, headers, page_size=100):
    """
    Tableau's API returns results in pages (like chapters in a book — you have to
    request one at a time). This helper keeps fetching pages until it has everything.
    Returns a flat list of all items found.
    """
    
    items = []
    page = 0
    total_available = 1  # start with 1 so the loop runs at least once

    while page * page_size < total_available:
        page += 1
        url = f"{base_url}?pageNumber={page}&pageSize={page_size}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        total_available = int(data["pagination"]["totalAvailable"])
        items += data[key][list(data[key].keys())[0]]  # handles "datasource" or "workbook" key

    return items


def filter_owned_items(items, token_username, dtype):
    """
    Splits a list of datasources or workbooks into two groups:
      - owned: items where the token user is the owner (safe to update)
      - skipped: items owned by someone else (we likely lack permission)

    Returns only the items owned by the token user — these are the ones
    we have permission to update. Items owned by others are skipped and
    a count is printed so the user knows they weren't forgotten.
    """
    owned = []
    skipped = []

    for item in items:
        owner = item.get("owner", {}).get("name", "")
        if owner.lower() == token_username.lower():
            owned.append(item)
        else:
            skipped.append(item)

    if skipped:
        print(f"  Skipping {len(skipped)} {dtype}(s) not owned by {token_username} "
              f"(no permission to update these).")

    return owned


def find_matching_connections(item, dtype, username, site_url, headers):
    """
    For a given datasource or workbook, fetch its connections and return any
    that match the target username AND use Snowflake.

    Think of each datasource/workbook as a cabinet — this function opens the
    cabinet and checks if any of the drawers (connections) belong to our user.
    """
    item_id = item.get("id")
    endpoint = "workbooks" if dtype == "workbook" else "datasources"
    url = f"{site_url}/{endpoint}/{item_id}/connections/"

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  WARNING: Could not fetch connections for {dtype} '{item.get('name')}' — skipping. ({e})")
        return []

    connections = r.json().get("connections", {}).get("connection")
    if not connections:
        return []

    # Build a small record for each connection that matches our user + snowflake
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
            # Add workbook URL if available
            if dtype == "workbook":
                record["url"] = item.get("webpageUrl")
            else:
                record["project"] = item.get("project", {}).get("name")

            matched.append(record)

    return matched


def update_connection(record, new_username, new_password, site_url, headers):
    """
    Send the updated password to Tableau for a single connection.
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
        print(f"  ERROR updating '{record['item_name']}': {e}")
        return False


def main():
    # Step 1 — Load credentials and settings from .env
    config = load_env_or_exit()

    new_username = config["sf_username"]
    new_password = config["sf_password"]
    old_username = config["sf_old_username"]

    # Determine mode based on whether sf_old_username is set.
    # In rotation mode, we search for connections using sf_username (password only).
    # In migration mode, we search for sf_old_username and swap to sf_username + new password.
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

    # Step 3 — Fetch all datasources and workbooks from the server
    print("Fetching all datasources from Tableau Server...")
    try:
        datasources = get_all_pages(f"{site_url}/datasources", "datasources", headers)
        print(f"  Found {len(datasources)} datasources total.")
    except Exception as e:
        print(f"ERROR fetching datasources: {e}")
        sys.exit(1)

    datasources = filter_owned_items(datasources, token_username, "datasource")
    print(f"  {len(datasources)} datasource(s) owned by you and eligible for update.")

    print("Fetching all workbooks from Tableau Server...")
    try:
        workbooks = get_all_pages(f"{site_url}/workbooks", "workbooks", headers)
        print(f"  Found {len(workbooks)} workbooks total.")
    except Exception as e:
        print(f"ERROR fetching workbooks: {e}")
        sys.exit(1)

    workbooks = filter_owned_items(workbooks, token_username, "workbook")
    print(f"  {len(workbooks)} workbook(s) owned by you and eligible for update.\n")

    # Step 4 — Find all connections that use our target user + Snowflake
    print(f"Scanning for Snowflake connections using username: {target_username}")
    matching_connections = []

    for ds in datasources:
        matches = find_matching_connections(ds, "datasource", target_username, 
                                            site_url, headers)
        matching_connections.extend(matches)

    for wb in workbooks:
        matches = find_matching_connections(wb, "workbook", target_username, 
                                            site_url, headers)
        matching_connections.extend(matches)

    if not matching_connections:
        key = "sf_old_username" if mode == "migration" else "sf_username"
        print(f"\nNo matching Snowflake connections found for user '{target_username}'.")
        print("Nothing to update. Double-check the sf_username value in your .env file.")
        sys.exit(0)

    print(f"\nFound {len(matching_connections)} connection(s) to update:\n")
    for i, cx in enumerate(matching_connections, 1):
        loc = cx.get("project") or cx.get("url") or "—"
        if mode == "migration":
            print(f"  {i}. [{cx['dtype'].upper()}] {cx['item_name']}  "
                  f"|  {cx['current_username']} → {new_username}  |  Location: {loc}")
        else:
            print(f"  {i}. [{cx['dtype'].upper()}] {cx['item_name']}  "
                  f"|  Owner: {cx['owner']}  |  Location: {loc}")
            
    # Step 5 — Update all matching connections
    action = "credentials" if mode == "migration" else "passwords"
    print(f"\nUpdating {action}...\n")
    success_count = 0
    fail_count = 0

    for cx in matching_connections:
        print(f"  Updating '{cx['item_name']}' ({cx['dtype']})...", end=" ")
        if update_connection(cx, new_username, new_password, site_url, headers):
            print("OK")
            success_count += 1
        else:
            fail_count += 1

    # Step 6 — Summary
    print(f"\n{'─'*50}")
    print(f"Done. {success_count} connection(s) updated successfully.")
    if fail_count:
        print(f"       {fail_count} connection(s) failed — see errors above.")
    print(f"{'─'*50}")


if __name__ == "__main__":
    main()