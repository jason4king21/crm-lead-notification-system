import json
import boto3

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

BUCKET_NAME = 'crm-lead-data'
queue_url = 'https://sqs.us-east-2.amazonaws.com/622025528750/lead-delay-queue'

def lambda_handler(event, context):
    body = json.loads(event.get('body', '{}'))
    lead_id = body.get('event', {}).get('lead_id', 'unknown')
    filename = f"crm_event_{lead_id}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"source/{filename}",
        Body=json.dumps(body),
        ContentType='application/json'
    )

    sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageBody=json.dumps({"lead_id": lead_id})
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Webhook received'})
    }
