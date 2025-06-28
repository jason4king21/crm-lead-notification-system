from diagrams import Diagram, Cluster
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.integration import SQS, SNS
from diagrams.aws.management import Cloudwatch
from diagrams.onprem.client import Users
from diagrams.onprem.network import Internet
from diagrams.aws.integration import SimpleNotificationServiceSnsEmailNotification as Email

with Diagram("architecture_diagram", show=False, direction="TB"):
    crm = Users("CRM Webhook\n(Close.com)")

    with Cluster("AWS"):
        with Cluster("S3 Bucket: crm-lead-data"):
            s3_source = S3("source/")
            s3_target = S3("target/")

        webhook_lambda = Lambda("Lambda: Webhook Handler")
        enrichment_lambda = Lambda("Lambda: Lead Lookup & Notify")
        delay_queue = SQS("SQS: lead-delay-queue")
        sns_topic = SNS("SNS: new-lead-notifications")
        logs = Cloudwatch("CloudWatch Logs")
        public_s3 = Internet("Public S3\n(dea-lead-owner)")
        email = Email("Email Alert\nvia SNS")

    # Flow
    crm >> webhook_lambda
    webhook_lambda >> s3_source
    webhook_lambda >> delay_queue
    delay_queue >> enrichment_lambda
    enrichment_lambda >> public_s3
    enrichment_lambda >> s3_target

    # Notifications
    enrichment_lambda >> sns_topic
    sns_topic >> email


    # Logging
    webhook_lambda >> logs
    enrichment_lambda >> logs
