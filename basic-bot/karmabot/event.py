import json
import re
import decimal
import os
import boto3
import urllib
import time
from boto3.dynamodb.conditions import Key

client = boto3.resource('dynamodb')
KARMA_TABLE = os.environ['KARMA_TABLE']
karma_table = client.Table(KARMA_TABLE)

BOT_TOKEN = os.environ['BOT_TOKEN']
SLACK_URL = "https://slack.com/api/chat.postMessage"


def receive(event, context):
    data = json.loads(event['body'])
    print("Got data: {}".format(data))
    return_body = "ok"

    if data["type"] == "url_verification":
        print("Received challenge")
        return_body = data["challenge"]
    elif (
        data["type"] == "event_callback" and
        data["event"]["type"] == "message" and
        "subtype" not in data["event"]
    ):
        handle_message(data)

    return {
        "statusCode": 200,
        "body": return_body
    }


def handle_message(data):
    poster_user_id = data["event"]["user"]

    # handle all ++'s and --'s
    p = re.compile(r"(<?@.+?>?)(\+\+|--)")
    m = p.findall(data["event"]["text"])
    if m:
        for match in m:
            karma_word = match[0].strip()
            if karma_word == "<@{}>".format(poster_user_id):
                print("A user tried to change his own karma")
                warning = "Hey {}, you can't change your own karma!".format(
                    karma_word
                )
                send_message(data, warning)
                continue

            if not karma_exists(karma_word):
                create_karma(karma_word)

            if match[1] == "++":
                new_value = karma_plus(karma_word)
                reply = "Well done! {} now at {}".format(
                    karma_word, new_value
                )
            elif match[1] == "--":
                new_value = karma_minus(karma_word)
                reply = "Awww :( {} now at {}".format(
                    karma_word, new_value
                )
            send_message(data, reply)

    # handle all messages like `@test_word ==`
    p = re.compile(r"(<?@.+?>?)==")
    m = p.findall(data["event"]["text"])
    if m:
        for match in m:
            karma_word = match.strip()
            karma = get_karma_for_id(karma_word)
            if karma is None:
                karma = 0
            reply = "Karma for {}: {}".format(
                karma_word, karma
            )
            send_message(data, reply)


def get_karma_for_id(karma_word):
    result = karma_table.get_item(
        Key={
            'karma_id': karma_word.lower()
        }
    )
    if "Item" in result:
        return int(result["Item"]["karma"])
    else:
        return None


def karma_plus(karma_word):
    print("Adding karma for {}".format(karma_word))
    return karma_mod(karma_word, "+")


def karma_minus(karma_word):
    print("Subtracting karma for {}".format(karma_word))
    return karma_mod(karma_word, "-")


def karma_exists(karma_word):
    response = karma_table.query(
        KeyConditionExpression=Key('karma_id').eq(
            karma_word.lower()
        )
    )
    return response["Count"] > 0


def create_karma(karma_word):
    print("First karma for {}".format(karma_word))
    timestamp = int(time.time() * 1000)
    item = {
        "karma_id": karma_word.lower(),
        "karma": 0,
        "createdAt": timestamp
    }

    karma_table.put_item(Item=item)


def karma_mod(karma_word, sign):
    response = karma_table.update_item(
        Key={
            "karma_id": karma_word.lower()
        },
        UpdateExpression="set karma = karma {} :val".format(sign),
        ExpressionAttributeValues={
            ':val': decimal.Decimal(1)
        },
        ReturnValues="UPDATED_NEW"
    )
    return response["Attributes"]["karma"]


def send_message(data, text):
    print("Sending message to Slack: {}".format(text))
    json_txt = json.dumps({
        "channel": data["event"]["channel"],
        "text": text
    }).encode('utf8')

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(BOT_TOKEN)
    }

    req = urllib.request.Request(
        SLACK_URL,
        data=json_txt,
        headers=headers
    )
    urllib.request.urlopen(req)
