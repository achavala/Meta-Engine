#!/usr/bin/env python3
"""Verify full automation pipeline."""
import sys, os, subprocess
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
from config import MetaConfig

print("=" * 60)
print("AUTOMATION VERIFICATION")
print("=" * 60)

# 1. Schedule
print("\n1. SCHEDULE:")
for i, rt in enumerate(MetaConfig.RUN_TIMES_ET):
    label = "Morning" if i == 0 else "Afternoon"
    print(f"   {label}: {rt} ET (Mon-Fri)")

# 2. Email
print("\n2. EMAIL:")
print(f"   SMTP Server: {MetaConfig.SMTP_SERVER}")
print(f"   SMTP User: {MetaConfig.SMTP_USER[:20]}..." if MetaConfig.SMTP_USER else "   SMTP User: NOT SET")
print(f"   Recipient: {MetaConfig.ALERT_EMAIL}")
print(f"   Password: {'SET (' + str(len(MetaConfig.SMTP_PASSWORD)) + ' chars)' if MetaConfig.SMTP_PASSWORD else 'NOT SET'}")
email_ok = bool(MetaConfig.SMTP_USER and MetaConfig.SMTP_PASSWORD and MetaConfig.ALERT_EMAIL)
print(f"   Status: {'✅ READY' if email_ok else '❌ NOT CONFIGURED'}")

# 3. X/Twitter
print("\n3. X/TWITTER:")
print(f"   API Key: {MetaConfig.X_API_KEY[:8]}..." if MetaConfig.X_API_KEY else "   API Key: NOT SET")
print(f"   Access Token: {MetaConfig.X_ACCESS_TOKEN[:8]}..." if MetaConfig.X_ACCESS_TOKEN else "   Access Token: NOT SET")
x_ok = bool(MetaConfig.X_API_KEY and MetaConfig.X_ACCESS_TOKEN)
print(f"   Status: {'✅ READY' if x_ok else '❌ NOT CONFIGURED'}")

# 4. Telegram
print("\n4. TELEGRAM:")
tg_ok = bool(MetaConfig.TELEGRAM_BOT_TOKEN and MetaConfig.TELEGRAM_CHAT_ID)
print(f"   Status: {'✅ READY' if tg_ok else '⏳ NOT CONFIGURED (will skip)'}")

# 5. launchd
result = subprocess.run(["launchctl", "list"], capture_output=True, text=True)
launchd_loaded = "com.metaengine.daily" in result.stdout
print(f"\n5. LAUNCHD AGENT:")
print(f"   Loaded: {'✅ YES' if launchd_loaded else '❌ NO'}")

# Check plist is in place
plist_path = os.path.expanduser("~/Library/LaunchAgents/com.metaengine.daily.plist")
print(f"   Plist exists: {'✅ YES' if os.path.exists(plist_path) else '❌ NO'}")

# 6. Pipeline flow
print("\n6. PIPELINE FLOW (what happens at 9:35 AM / 3:15 PM):")
print("   launchd triggers -> run_meta_engine.py --force")
print("   -> run_meta_engine() -> _run_pipeline()")
print("     Step 1: Get PutsEngine Top 10")
print("     Step 2: Get Moonshot Top 10")
print("     Step 3: Cross-Engine Analysis")
print("     Step 4: Generate Summaries")
print("     Step 5: Generate Chart + Markdown Report")
print(f"     Step 6: Send Email          -> {'✅ AUTO' if email_ok else '❌ SKIP'}")
print(f"     Step 7: Send Telegram       -> {'✅ AUTO' if tg_ok else '⏳ SKIP'}")
print(f"     Step 8: Post to X/Twitter   -> {'✅ AUTO' if x_ok else '❌ SKIP'}")

print("\n" + "=" * 60)
if email_ok and x_ok and launchd_loaded:
    print("✅ ALL SYSTEMS GO! Email + X will fire automatically at 9:35 AM & 3:15 PM ET.")
else:
    issues = []
    if not email_ok: issues.append("Email")
    if not x_ok: issues.append("X/Twitter")
    if not launchd_loaded: issues.append("launchd")
    print(f"❌ Issues: {', '.join(issues)}")
print("=" * 60)
