# Tableau Snowflake Password Updater

When a Snowflake password changes, any Tableau datasource or workbook with that password embedded will break until it's manually updated. This tool automates that process — scanning your Tableau Server for all affected connections and updating them in bulk, so you don't have to track them down and fix them one by one.

---

## What's in this repo

| File | Purpose |
|------|---------|
| `src/update_tableau_password.py` | The **full script** — updates all eligible datasources and workbooks in one run |
| `src/update_tableau_password_TEST.py` | The **test script** — runs on a single named item so you can verify everything works before running the full script |
| `.env.example` | A template showing all the required configuration values |
| `.gitignore` | Prevents your `.env` file (which contains credentials) from being accidentally committed |

---

## How it works

### Full script (`update_tableau_password.py`)

1. Authenticates with your Tableau Server using a Personal Access Token
2. Fetches every datasource and workbook on the site
3. Filters down to only items **you own** (since you can only update credentials on assets you own)
4. Scans those items for Snowflake connections that use the specified username
5. Updates the embedded password on every matching connection
6. Prints a summary of what was updated and flags any failures

### Test script (`update_tableau_password_TEST.py`)

Works exactly the same way as the full script, but scoped to a **single datasource or workbook** that you specify by name. It also pauses before making any changes and asks you to confirm. The intended workflow is:

> Run the test script → verify the item works in Tableau → run the full script

---

## Prerequisites

Before running either script, you'll need the following installed on your machine:

- **Python 3.8 or higher** — [Download here](https://www.python.org/downloads/)
- **The following Python packages**, which can be installed by running this command in your terminal:

```bash
pip install requests python-dotenv
```

---

## Setup

### Step 1 — Create your `.env` file

The scripts read all credentials from a `.env` file that lives in the same folder as the scripts. This keeps sensitive information out of the code itself.

Copy the provided template to get started:

```bash
cp .env.example .env
```

Then open `.env` in any text editor and fill in your values. See the [Configuration reference](#configuration-reference) section below for details on each key.

> ⚠️ **Important:** Never share your `.env` file or commit it to GitHub. The included `.gitignore` file prevents this automatically, but it's worth being aware of.

### Step 2 — Create a Tableau Personal Access Token

The scripts authenticate with Tableau using a Personal Access Token (PAT) rather than your username and password. Here's how to create one:

1. Sign in to your Tableau Server in a web browser
2. Click your profile icon in the top-right corner and select **My Account Settings**
3. Scroll down to the **Personal Access Tokens** section
4. Enter a name for your token (e.g. `password-updater`) and click **Create new token**
5. Copy the **Token Name** and **Token Secret** that appear — the secret is only shown once, so save it somewhere safe
6. Paste the Token Name into the `ts_token` field in your `.env` file, and the Token Secret into `ts_secret`

> **Note:** PATs inherit your permissions in Tableau. The scripts will only be able to update assets that you own.

### Step 3 — Run the test script first

Before running the full script, it's strongly recommended to test on a single item you own:

1. Open `update_one_password.py` in a text editor
2. Set `TEST_ITEM_NAME` to the exact name of a datasource or workbook as it appears in Tableau (capitalization matters)
3. Set `TEST_ITEM_TYPE` to either `"datasource"` or `"workbook"`
4. Run the script:

```bash
python src/update_one_password.py
```

5. When prompted, type `YES` to confirm the update
6. Log into Tableau and verify the item loads data correctly

### Step 4 — Run the full script

Once you've confirmed the test worked:

```bash
python src/update_tableau_passwords.py
```

The script will print a list of every connection it's going to update, then proceed automatically. A summary at the end shows how many succeeded and flags any that failed.

---

## Configuration reference

All configuration lives in your `.env` file. Here is a description of each key:

| Key | Description | Example |
|-----|-------------|---------|
| `ts_token` | The name of your Tableau Personal Access Token | `password-updater` |
| `ts_secret` | The secret value of your Tableau Personal Access Token | `xxxx-xxxx-xxxx` |
| `api_version` | The Tableau REST API version your server uses | `3.19` |
| `server_url` | The base URL of your Tableau Server, no trailing slash | `https://tableau.pitt.edu` |
| `sf_username` | The Snowflake username whose password needs updating | `NAS230@PITT.EDU` |
| `sf_password` | The new Snowflake password to embed in all matching connections | `your-new-password` |

If you're not sure which API version your server uses, you can find it in Tableau Server's admin settings, or ask your Tableau Server administrator.

---

## Example `.env` file

```
ts_token=password-updater
ts_secret=xxxx-xxxx-xxxx-xxxx
api_version=3.19
server_url=https://tableau.pitt.edu
sf_username=NAS230@PITT.EDU
sf_password=your-new-snowflake-password
```

---

## Important notes

- **Ownership requirement:** The scripts only update assets you own. Any datasource or workbook owned by someone else will be skipped automatically. If another team member also needs to update their assets, they should run the script using their own Personal Access Token.
- **Embedded connections only:** The scripts update *embedded* Snowflake credentials — meaning the password is stored directly on the datasource or workbook. If a workbook connects to a *published datasource* rather than directly to Snowflake, the password should be updated on the published datasource instead, and the workbook will pick up the change automatically.
- **The `.env` file is local only:** It is listed in `.gitignore` and will not be committed to GitHub. Each person who runs these scripts needs to create their own `.env` file on their own machine.