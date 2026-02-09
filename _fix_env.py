#!/usr/bin/env python3
"""Fix .env to use comma-separated email recipients."""
import os

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
lines = open(env_path).readlines()

new_lines = []
alert_emails = []

for line in lines:
    if line.strip().startswith("META_ALERT_EMAIL="):
        email = line.strip().split("=", 1)[1]
        if email not in alert_emails:
            alert_emails.append(email)
    else:
        new_lines.append(line)

# Insert combined line after META_SMTP_PASSWORD
combined = ",".join(alert_emails)
for i, line in enumerate(new_lines):
    if "META_SMTP_PASSWORD" in line:
        new_lines.insert(i + 1, "META_ALERT_EMAIL=" + combined + "\n")
        break

with open(env_path, "w") as f:
    f.writelines(new_lines)

print(f"Updated .env with combined recipients: {combined}")
