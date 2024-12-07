from datetime import datetime, timezone
import json
import logging
from typing import List
import boto3
import requests

from utils import (
    download_file_content_from_s3,
    get_item_from_dynamodb,
    send_log_to_cloudwatch,
    send_message_to_sqs,
    send_pages_to_promptflow,
    sqs_codes,
    start_ocr_analysis,
    update_dynamo_table_item,
    upload_item_to_dynamodb,
)


sqs_client = boto3.client(
    "sqs", endpoint_url="http://localhost:4566", region_name="eu-west-3"
)
s3_client = boto3.client(
    "s3", endpoint_url="http://localhost:4566", region_name="eu-west-3"
)
# AWS RESOURCES CONFIG
REGION_NAME = "eu-west-3"
S3_BUCKET_NAME = "files-bucket"
SQS_QUEUE_NAME = "files-queue"
DYNAMODB_TABLE_NAME = "FilesTable"
LOG_GROUP_NAME = "pipeline_logger"
LOG_STREAM_NAME = "pipeline_streamer"
VIRTUAL_HOST_PORT = 4566
SQS_QUEUE_URL = f"http://sqs.{REGION_NAME}.localhost.localstack.cloud:{VIRTUAL_HOST_PORT}/000000000000/{SQS_QUEUE_NAME}"
S3_BUCKET_URL = f"http://{S3_BUCKET_NAME}.s3.{REGION_NAME}.localhost.localstack.cloud:{VIRTUAL_HOST_PORT}/"


# # CONFIG LOGGER
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler(f"{LOGGER_PATH}/pipeline.log"),
#         logging.StreamHandler(),
#     ],
# )
# logger = logging.getLogger("document_pipeline")


# # CONFIG CLIENTS
# s3_client = boto3.client(
#     "s3", endpoint_url="http://localhost:4566", region_name="eu-west-3"
# )
# dynamodb = boto3.resource(
#     "dynamodb", endpoint_url="http://localhost:4566", region_name="eu-west-3"
# )
# sqs_client = boto3.client(
#     "sqs", endpoint_url="http://127.0.0.1:4566", region_name="eu-west-3"
# )
# logs_client = boto3.client(
#     "logs", endpoint_url="http://127.0.0.1:4566", region_name="eu-west-3"
# )

# # AWS RESOURCES CONFIG
# REGION_NAME = "eu-west-3"
# S3_BUCKET_NAME = "files-bucket"
# SQS_QUEUE_NAME = "files-queue"
# DYNAMODB_TABLE_NAME = "FilesTable"
# LOG_GROUP_NAME = "pipeline_logger"
# LOG_STREAM_NAME = "pipeline_streamer"
# VIRTUAL_HOST_PORT = 4566
# SQS_QUEUE_URL = f"http://sqs.{REGION_NAME}.127.0.0.1.localstack.cloud:{VIRTUAL_HOST_PORT}/000000000000/{SQS_QUEUE_NAME}"
# S3_BUCKET_URL = f"http://{S3_BUCKET_NAME}.s3.{REGION_NAME}.127.0.0.1.localstack.cloud:{VIRTUAL_HOST_PORT}/"

# # CODES
# CLIENT_UPLOAD_CODE = "client_upload"


def listen_tasks_from_sqs() -> None:
    while True:
        print("iteration")
        messages = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=3, WaitTimeSeconds=10
        )
        print("messages: ", messages)
        if "Messages" in messages:
            for message in messages["Messages"]:
                receipt_handle = message["ReceiptHandle"]
                body = message["Body"]
                if not body:
                    send_log_to_cloudwatch("malformed SQS message no body, deleted")
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )
                    continue
                body = json.loads(body)
                if sqs_codes.NEW_FILE_CODE.value in body:
                    file_id = body[sqs_codes.NEW_FILE_CODE.value].get("file_id")
                    file_key = body[sqs_codes.NEW_FILE_CODE.value].get("file_key")
                    if not file_id:
                        send_log_to_cloudwatch(
                            "malformed SQS message no file_id, deleted"
                        )
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )
                        continue
                    # get_item_from_dynamodb(
                    #     DYNAMODB_TABLE_NAME, key={"file_id": {"S": file_id}}
                    # )
                    file_content = download_file_content_from_s3(file_key)
                    send_log_to_cloudwatch(
                        f"file: {file_key} downloaded for OCR analysis"
                    )
                    pages = start_ocr_analysis(file_key, file_content)
                    send_log_to_cloudwatch(f"OCR over for file: {file_key}")
                    update_expression = "SET ocr_pages = :ocr"
                    expression_attributes = {
                        ":ocr": {"L": [{"S": page} for page in pages]}
                    }
                    update_dynamo_table_item(
                        file_id, update_expression, expression_attributes
                    )
                    send_message_to_sqs(
                        SQS_QUEUE_URL,
                        {sqs_codes.OCR_COMPLETED.value: {"file_id": file_id}},
                    )
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )
                elif sqs_codes.OCR_COMPLETED.value in body:
                    file_id = body[sqs_codes.OCR_COMPLETED.value].get("file_id")
                    if not file_id:
                        send_log_to_cloudwatch(
                            "malformed SQS message no file_id, deleted"
                        )
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )
                        continue
                    item = get_item_from_dynamodb(
                        DYNAMODB_TABLE_NAME, key={"file_id": {"S": file_id}}
                    )
                    send_log_to_cloudwatch(
                        f"dynamodb interrogated for file: {file_id} before classification"
                    )
                    classification = send_pages_to_promptflow(
                        item["Item"].get("ocr_pages")
                    )
                    send_log_to_cloudwatch(f"classification over for file: {file_id}")
                    update_expression = "SET classification_result = :classification"
                    # expression_attributes = {
                    #     ":classification": {
                    #         "segmentDiag": {
                    #             "L": [
                    #                 {"S": segment}
                    #                 for segment in classification["segmentDiag"]
                    #             ]
                    #         },
                    #         "segmentOp": {
                    #             "L": [
                    #                 {"S": segment}
                    #                 for segment in classification["segmentOp"]
                    #             ]
                    #         },
                    #     }
                    # }
                    expression_attributes = {
                        ":classification": {
                            "M": {
                                "segmentDiag": {
                                    "L": [
                                        {
                                            "M": {
                                                "categorie": {"S": seg["categorie"]},
                                                "pages": {
                                                    "L": [
                                                        {"N": str(page)}
                                                        for page in seg["pages"]
                                                    ]
                                                },
                                            }
                                        }
                                        for seg in classification["segmentDiag"]
                                    ]
                                },
                                "segmentOp": {
                                    "L": [
                                        {
                                            "M": {
                                                "categorie": {"S": seg["categorie"]},
                                                "pages": {
                                                    "L": [
                                                        {"N": str(page)}
                                                        for page in seg["pages"]
                                                    ]
                                                },
                                            }
                                        }
                                        for seg in classification["segmentOp"]
                                    ]
                                },
                            }
                        }
                    }

                    print(expression_attributes)
                    update_dynamo_table_item(
                        file_id, update_expression, expression_attributes
                    )
                    # send_message_to_sqs(
                    #     SQS_QUEUE_URL,
                    #     {
                    #         sqs_codes.CLASSIFICATION_COMPLETED.value: {
                    #             "file_id": file_id
                    #         }
                    #     },
                    # )
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )

                # elif sqs_codes.CLASSIFICATION_COMPLETED.value in body:
                #     file_id = body[sqs_codes.CLASSIFICATION_COMPLETED.value].get(
                #         "file_id"
                #     )
                #     if not file_id:
                #         send_log_to_cloudwatch(
                #             "malformed SQS message no file_id, deleted"
                #         )
                #         sqs_client.delete_message(
                #             QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                #         )
                #         continue
                #     file_content = download_file_content_from_s3(file_key)


listen_tasks_from_sqs()
