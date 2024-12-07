from datetime import datetime, timezone
from enum import Enum
import io
import json
import logging
import os
from typing import Dict, List

import boto3
import botocore
from pypdf import PdfReader, PdfWriter
import requests

LOGGER_PATH = "logs"
# CONFIG LOGGER
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{LOGGER_PATH}/pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("document_pipeline")
# CONFIG CLIENTS
s3_client = boto3.client(
    "s3", endpoint_url="http://localhost:4566", region_name="eu-west-3"
)
dynamodb = boto3.client(
    "dynamodb", endpoint_url="http://localhost:4566", region_name="eu-west-3"
)
sqs_client = boto3.client(
    "sqs", endpoint_url="http://127.0.0.1:4566", region_name="eu-west-3"
)
logs_client = boto3.client(
    "logs", endpoint_url="http://127.0.0.1:4566", region_name="eu-west-3"
)

# AWS RESOURCES CONFIG
REGION_NAME = "eu-west-3"
S3_BUCKET_NAME = "files-bucket"
SQS_QUEUE_NAME = "files-queue"
DYNAMODB_TABLE_NAME = "FilesTable"
LOG_GROUP_NAME = "pipeline_logger"
LOG_STREAM_NAME = "pipeline_streamer"
VIRTUAL_HOST_PORT = 4566
SQS_QUEUE_URL = f"http://sqs.{REGION_NAME}.127.0.0.1.localstack.cloud:{VIRTUAL_HOST_PORT}/000000000000/{SQS_QUEUE_NAME}"
S3_BUCKET_URL = f"http://{S3_BUCKET_NAME}.s3.{REGION_NAME}.127.0.0.1.localstack.cloud:{VIRTUAL_HOST_PORT}/"


# CODES
class sqs_codes(Enum):
    NEW_FILE_CODE = "new_file_uploaded"
    OCR_COMPLETED = "file_ocr_completed"
    CLASSIFICATION_COMPLETED = "classification_completed"


def send_log_to_cloudwatch(message: str) -> None:
    # timestamp = datetime.now(timezone.utc)
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    logs_client.put_log_events(
        logGroupName=LOG_GROUP_NAME,
        logStreamName=LOG_STREAM_NAME,
        logEvents=[
            {"timestamp": timestamp, "message": message},
        ],
    )


def upload_item_to_dynamodb(table_name: str, item: Dict) -> None:
    dynamodb.put_item(TableName=table_name, Item=item)


def update_dynamo_table_item(
    file_id: str, update_expression: str, expression_attributes: str
):
    dynamodb.update_item(
        TableName=DYNAMODB_TABLE_NAME,
        Key={"file_id": {"S": file_id}},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attributes,
        ReturnValues="NONE",
    )


def get_item_from_dynamodb(table_name: str, key: Dict) -> str:
    return dynamodb.get_item(TableName=table_name, Key=key)


def upload_file_to_s3(file_content: bytes, file_key: str) -> None:
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=file_content)
        logger.info(f"{file_key} uploaded to S3 successfully")
        send_log_to_cloudwatch(f"Document {file_key} uploaded to S3 successfully")
    except Exception as err:
        logger.error(f"Upload error: {file_key} not uploaded to S3: {err}")
        send_log_to_cloudwatch(f"Upload error: {file_key} not uploaded to S3: {err}")


def clean_aws_resources() -> None:
    try:
        dynamodb.delete_table(TableName=DYNAMODB_TABLE_NAME)
        print(f"Table {DYNAMODB_TABLE_NAME} deleted.")
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Table {DYNAMODB_TABLE_NAME} does not exist.")

    try:
        queue_url_response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
        sqs_client.delete_queue(QueueUrl=queue_url_response["QueueUrl"])
        print(f"Queue {SQS_QUEUE_NAME} deleted.")
    except sqs_client.exceptions.QueueDoesNotExist:
        print(f"Queue {SQS_QUEUE_NAME} does not exist.")

    try:
        bucket = s3_client.list_objects(Bucket=S3_BUCKET_NAME)
        objects = [{"Key": obj["Key"]} for obj in bucket.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=S3_BUCKET_NAME, Delete={"Objects": objects})
        s3_client.delete_bucket(Bucket=S3_BUCKET_NAME)
        print(f"Bucket {S3_BUCKET_NAME} and all its contents deleted.")
    except s3_client.exceptions.NoSuchBucket:
        print(f"Bucket {S3_BUCKET_NAME} does not exist.")

    try:
        streams = logs_client.describe_log_streams(logGroupName=LOG_GROUP_NAME)[
            "logStreams"
        ]
        for stream in streams:
            logs_client.delete_log_stream(
                logGroupName=LOG_GROUP_NAME, logStreamName=stream["logStreamName"]
            )
        logs_client.delete_log_group(logGroupName=LOG_GROUP_NAME)
        print(f"Log group {LOG_GROUP_NAME} and all its streams deleted.")
    except logs_client.exceptions.ResourceNotFoundException:
        print(f"Log group {LOG_GROUP_NAME} does not exist.")


def create_aws_resources() -> None:
    # response = dynamodb.delete_table(TableName=DYNAMODB_TABLE_NAME)

    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP_NAME)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    try:
        logs_client.create_log_stream(
            logGroupName=LOG_GROUP_NAME, logStreamName=LOG_STREAM_NAME
        )
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    try:
        s3_client.create_bucket(
            Bucket=S3_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-3"},
        )
    except (
        s3_client.exceptions.BucketAlreadyExists,
        s3_client.exceptions.BucketAlreadyOwnedByYou,
    ):
        pass
    try:
        response = sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)
        print(f"Queue created: {response['QueueUrl']}")
    except sqs_client.exceptions.QueueNameExists:
        pass
    try:
        table = dynamodb.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[{"AttributeName": "file_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
    except botocore.exceptions.ClientError as err:
        print("err:", err)
        pass


def send_message_to_sqs(sqs_queue_url: str, data: str) -> None:
    response = sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps(data, ensure_ascii=False),
    )
    logger.info(f"Message sent to SQS : {response['MessageId']}")
    send_log_to_cloudwatch(f"Message sent to SQS : {response['MessageId']}")


def start_ocr_analysis(filename: str, file_content: bytes) -> List[str]:
    try:
        response = requests.post(
            "http://127.0.0.1:8000/ocr",
            files=[("pdf", (filename, file_content, "application/pdf"))],
        )
        response.raise_for_status()
        pages = response.json()["pages"]
        logger.info(f"OCR received for {filename}: {pages}")
        send_log_to_cloudwatch(f"OCR received for {filename}: {pages}")
        return pages
    except requests.RequestException as err:
        logger.error(f"Failed to make OCR request for {filename}: {str(err)}")
        send_log_to_cloudwatch(f"Failed to make OCR request for {filename}: {str(err)}")


def send_pages_to_promptflow(pages: List[str]) -> Dict:
    try:
        response = requests.post("http://127.0.0.1:8000/score", json={"pages": pages})
        response.raise_for_status()
        results = {k: v for k, v in response.json().items() if "segment" in k}
        logger.info(f"Classification received: {results}")
        send_log_to_cloudwatch(f"Classification received: {results}")
        return results
    except requests.RequestException as err:
        logger.error(f"Failed to send pages to PromptFlow: {str(err)}")
        send_log_to_cloudwatch(f"Failed to send pages to PromptFlow: {str(err)}")
        return {}


def save_classification(
    filename: str, file_content: bytes, classification: Dict, target_folder: Dict
) -> None:
    try:
        pdf = PdfReader(io.BytesIO(file_content))
        for segment in classification["segmentDiag"] + classification["segmentOp"]:
            writer = PdfWriter()
            page = segment["pages"][0]
            while page <= segment["pages"][1]:
                writer.add_page(pdf.pages[page - 1])
                page += 1
            output_path = f"{target_folder}/{filename}_{segment['categorie']}_{segment['pages'][0]}-{segment['pages'][1]}.pdf"
            with open(
                output_path,
                "wb",
            ) as f:
                writer.write(f)
                logger.info(f"Saved classified segment: {output_path}")
                send_log_to_cloudwatch(f"Saved classified segment: {output_path}")
    except Exception as err:
        logger.error(f"Failed to save classified PDF for {filename}: {str(err)}")
        send_log_to_cloudwatch(
            f"Failed to save classified PDF for {filename}: {str(err)}"
        )


def get_folder_files(folder_path: str) -> List[str]:
    return os.listdir(folder_path)


def start_ocr_analysis(filename: str, file_content: bytes) -> List[str]:
    try:
        response = requests.post(
            "http://127.0.0.1:8000/ocr",
            files=[("pdf", (filename, file_content, "application/pdf"))],
        )
        response.raise_for_status()
        pages = response.json()["pages"]
        logger.info(f"OCR received for {filename}: {pages}")
        send_log_to_cloudwatch(f"OCR received for {filename}: {pages}")
        upload_file_to_s3(file_content, f"OCR_{filename}")
        return pages
    except requests.RequestException as err:
        logger.error(f"Failed to make OCR request for {filename}: {str(err)}")
        send_log_to_cloudwatch(f"Failed to make OCR request for {filename}: {str(err)}")


def download_file_content_from_s3(file_key: str) -> bytes:
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        file_content = response["Body"].read()
        logger.info(f"{file_key} downloaded from S3")
        send_log_to_cloudwatch(f"Document {file_key} downloaded from S3")
        return file_content
    except Exception as err:
        logger.info(f"Download error: {file_key} not downloaded from S3: {err}")
        send_log_to_cloudwatch(
            f"Download error: {file_key} not downloaded from S3: {err}"
        )
        return
