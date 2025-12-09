import requests
WEBHOOK_URL = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token"


def send_error_to_discord(error_message):
    """Send error message to Discord."""
    # note for dev
    # this is just temp func can use aws ses for handle error
    data = {
        "content": f"⚠️ **Error Alert:**\n```\n{error_message}\n```"
    }
    try:
        response = requests.post(WEBHOOK_URL, json=data)
    except requests.exceptions.RequestException as e:
        print("Failed to send message to Discord:", e)
