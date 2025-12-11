import requests

WEBHOOK_URL = "https://discord.com/api/webhooks/1448576119447097384/CsAGx3dg3nHLvk-44tp0itGu6F0g1TugHfboUsyyzEdsS5hPD6v6b3Op0Yd5QaGXQTYQ"


def send_error_to_discord(error_message):
    """Send error message to Discord."""
    # note for dev
    # this is just temp func can use aws ses for handle error
    data = {"content": f"⚠️ **Error Alert:**\n```\n{error_message}\n```"}
    try:
        response = requests.post(WEBHOOK_URL, json=data)
    except requests.exceptions.RequestException as e:
        print("Failed to send message to Discord:", e)
