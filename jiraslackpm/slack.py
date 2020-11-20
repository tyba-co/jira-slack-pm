import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web import SlackResponse


class SlackClient(object):
    def __init__(self, token=None):
        self.client = WebClient(token=token or os.environ['SLACK_OAUTH_ACCESS_TOKEN'])

    def post_message_to_channel(self, channel: str, message: str):
        try:
            response = self.client.chat_postMessage(channel=channel, text=message)
            assert response["message"]["text"] == "Hello world!"
            return response.get('message')
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            print(f"Got an error: {e.response['error']}")

    def post_blocks_message_to_channel(self, channel: str, blocks: list):
        try:
            response = self.client.chat_postMessage(channel=channel, blocks=blocks)
            return response.get('message')
        except SlackApiError as e:
            print(f"Got an error: {e.response['error']}")

    def create_direct_message(self, users: list):
        response = self.client.conversations_open(users=users)
        if response.data.get('ok'):
            return response.get('channel')

    def get_user_by_email(self, email: str) -> SlackResponse:
        user = self.client.api_call(api_method='users.lookupByEmail', params={"email": email})
        if user.data.get('ok'):
            return user.data.get('user')
