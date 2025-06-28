import boto3
import json
import urllib.request
import smtplib
from email.mime.text import MIMEText

s3 = boto3.client('s3')
sns = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:622025528750:new-lead-notifications'

# Bucket info
RAW_BUCKET = "crm-lead-data"
RAW_PREFIX = "source/"
TARGET_PREFIX = "target/"
# LOOKUP_BUCKET = "dea-lead-owner"

PUBLIC_LOOKUP_BUCKET = "dea-lead-owner"
PUBLIC_LOOKUP_URL = f"https://{PUBLIC_LOOKUP_BUCKET}.s3.us-east-1.amazonaws.com"

SENDER = "jason4king21@gmail.com"
RECIPIENT = "jason4king21@gmail.com"
SUBJECT = "📥 New Lead Alert"

def send_sns_notification(lead_info):
    subject = f"📥 New Lead: {lead_info.get('display_name', 'N/A')}"
    message = f"""
New Lead Alert

Name: {lead_info.get('display_name', 'N/A')}
Lead ID: {lead_info.get('lead_id', 'N/A')}
Created Date: {lead_info.get('date_created', 'N/A')}
Label: {lead_info.get('status_label', 'N/A')}
Email: {lead_info.get('lead_email', 'N/A')}
Lead Owner: {lead_info.get('lead_owner', 'N/A')}
Funnel: {lead_info.get('funnel', 'N/A')}
"""
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            msg = json.loads(record['body'])
            lead_id = msg.get('lead_id')

            if not lead_id:
                print("❌ No lead_id found in message.")
                continue

            # Read raw lead JSON
            raw_key = f"{RAW_PREFIX}crm_event_{lead_id}.json"
            raw_obj = s3.get_object(Bucket=RAW_BUCKET, Key=raw_key)
            raw_data = json.loads(raw_obj["Body"].read())

            # Read lead from public S3 URL
            lead_url = f"{PUBLIC_LOOKUP_URL}/{lead_id}.json"
            print(f"Fetching lead from {lead_url}")
            response = urllib.request.urlopen(lead_url)
            enrichment = json.loads(response.read())

            # Merge enrichment
            raw_data["event"]["data"]["lead_owner"] = enrichment.get("lead_owner", "Unassigned")

            # Write enriched data
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=f"{TARGET_PREFIX}crm_lead_{lead_id}.json",
                Body=json.dumps(raw_data),
                ContentType="application/json"
            )

            print(f"✅ Processed lead: {lead_id}")

            # Send SNS notification
            send_sns_notification({
                "display_name": raw_data["event"]["data"].get("display_name", ""),
                "lead_id": lead_id,
                "date_created": raw_data["event"]["data"].get("date_created", ""),
                "status_label": raw_data["event"]["data"].get("status_label", ""),
                "lead_email": enrichment.get("lead_email", ""),
                "lead_owner": enrichment.get("lead_owner", ""),
                "funnel": enrichment.get("funnel", "")
            })

        except urllib.error.HTTPError as e:
            print(f"❌ HTTP error for {lead_id}: {e.code} {e.reason}")
        except ClientError as ce:
            print(f"❌ S3 error for {lead_id}: {str(ce)}")
        except Exception as e:
            print(f"❌ General error for {lead_id}: {str(e)}")

    return {"statusCode": 200}
