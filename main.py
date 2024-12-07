import os
import argparse
from typing import List
from uuid import uuid4

from utils import (
    DYNAMODB_TABLE_NAME,
    SQS_QUEUE_URL,
    clean_aws_resources,
    create_aws_resources,
    send_log_to_cloudwatch,
    send_message_to_sqs,
    sqs_codes,
    upload_file_to_s3,
    logger,
    upload_item_to_dynamodb,
)


def main(file_paths: List[str]) -> None:
    for path in file_paths:
        try:
            filename = os.path.basename(path)
            with open(path, "rb") as f:
                file_content = f.read()
            upload_file_to_s3(file_content, filename)
            logger.info(f"file uploaded to s3: filename: {filename}")
            send_log_to_cloudwatch(f"file uploaded to s3: filename: {filename}")
            file_id = str(uuid4())
            upload_item_to_dynamodb(
                DYNAMODB_TABLE_NAME,
                {
                    "file_id": {"S": file_id},
                    "original_file": {"S": f"s3://files-bucket/{filename}"},
                },
            )
            logger.info(f"file uploaded to dynamodb: filename: {filename}")
            send_log_to_cloudwatch(f"file uploaded to dynamodb: filename: {filename}")
            send_message_to_sqs(
                SQS_QUEUE_URL,
                {
                    sqs_codes.NEW_FILE_CODE.value: {
                        "file_id": file_id,
                        "file_key": filename,
                    }
                },
            )
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            send_log_to_cloudwatch(f"File not found: {path}")
        except Exception as err:
            logger.error(f"Technical error processing file: {err}")
            send_log_to_cloudwatch(f"Technical issue: {err}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze documents in the pipeline")
    parser.add_argument(
        "--files",
        type=str,
        help="Names of the documents to classify",
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--clean", action="store_true", help="Clean cloud (localstack) resources"
    )
    parser.add_argument(
        "--create", action="store_true", help="Create cloud (localstack) resources"
    )
    args = parser.parse_args()
    if not args.files:
        logger.error(f"cli lacking parameters: {args}")
        raise Exception("cli lacking parameters")
    if args.clean:
        clean_aws_resources()
    if args.create:
        create_aws_resources()

    main(args.files)
