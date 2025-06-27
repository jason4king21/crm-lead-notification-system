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
LOOKUP_BUCKET = "dea-lead-owner"


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

            # Read enrichment data
            lookup_key = f"dea-lead-owner/{lead_id}.json"
            lookup_obj = s3.get_object(Bucket=RAW_BUCKET, Key=lookup_key)
            enrichment = json.loads(lookup_obj["Body"].read())

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

        except Exception as e:
            print(f"❌ Error processing lead {lead_id}: {str(e)}")
            # Optionally publish to a DLQ or CloudWatch for alerting

    return {"statusCode": 200}
