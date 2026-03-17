from pathlib import Path
from playwright.sync_api import sync_playwright

state_path = Path(r"C:\osha_data\secrets\ohs_bg_storage_state.json")
user_data_dir = Path(r"C:\osha_data\secrets\ohs_bg_edge_profile")
state_path.parent.mkdir(parents=True, exist_ok=True)
user_data_dir.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        channel="msedge",
        headless=False,
    )
    page = context.new_page()
    page.goto("https://buyersguide.ohsonline.com/", wait_until="domcontentloaded")

    print("")
    print("Use the real Edge window.")
    print("1. Complete the Cloudflare verification")
    print("2. Sign in with the OHS Buyers Guide account")
    print("3. Confirm you are actually signed in")
    print("4. Press Enter here to save storage state")
    input()

    context.storage_state(path=str(state_path))
    print(f"SAVED:{state_path}")
    context.close()
