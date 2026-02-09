#!/usr/bin/env python3
"""Send email to both recipients using the latest report."""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
from config import MetaConfig

print(f"Recipients: {MetaConfig.ALERT_EMAIL}")

# Load summaries
with open("output/summaries_20260209.json") as f:
    summaries = json.load(f)

# Find latest report and chart
report_md = "output/meta_engine_report_20260209.md"
chart_path = None
for fname in sorted(os.listdir("output"), reverse=True):
    if fname.startswith("meta_engine_chart_") and fname.endswith(".png"):
        chart_path = os.path.join("output", fname)
        break

print(f"Report: {report_md}")
print(f"Chart: {chart_path}")

from notifications.email_sender import send_meta_email
result = send_meta_email(
    summaries=summaries,
    chart_path=chart_path,
    report_md_path=report_md,
    smtp_server=MetaConfig.SMTP_SERVER,
    smtp_port=MetaConfig.SMTP_PORT,
    smtp_user=MetaConfig.SMTP_USER,
    smtp_password=MetaConfig.SMTP_PASSWORD,
    recipient=MetaConfig.ALERT_EMAIL,
)

if result:
    print(f"\n✅ Email sent to BOTH recipients!")
else:
    print(f"\n❌ Email failed!")
