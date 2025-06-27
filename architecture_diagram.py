from diagrams import Diagram, Cluster
from diagrams.aws.integration import Eventbridge
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.integration import SQS, SNS
from diagrams.aws.management import Cloudwatch
from diagrams.onprem.client import Users


with Diagram("CRM Lead Processing & Notification System", show=False, direction="TB"):
    crm = Users("CRM Webhook\n(Close.com)")

    with Cluster("AWS"):
        with Cluster("S3 Bucket: crm-lead-data"):
            s3_source = S3("source/")
            s3_lookup = S3("dea-lead-owner/")
            s3_target = S3("target/")

        webhook_lambda = Lambda("Lambda: Webhook Handler")
        enrichment_lambda = Lambda("Lambda: Enrich & Notify")
        delay_queue = SQS("SQS: lead-delay-queue")
        sns_topic = SNS("SNS: new-lead-notifications")
        logs = Cloudwatch("CloudWatch Logs")

    crm >> webhook_lambda
    webhook_lambda >> s3_source
    webhook_lambda >> delay_queue
    delay_queue >> enrichment_lambda
    enrichment_lambda >> s3_lookup
    enrichment_lambda >> s3_target
    enrichment_lambda >> sns_topic
    webhook_lambda >> logs
    enrichment_lambda >> logs
