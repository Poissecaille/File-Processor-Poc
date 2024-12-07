import json
from utils import (
    DYNAMODB_TABLE_NAME,
    SQS_DLQ_QUEUE_URL,
    SQS_QUEUE_URL,
    download_file_content_from_s3,
    get_item_from_dynamodb,
    save_classification_segments,
    send_log_to_cloudwatch,
    send_message_to_sqs,
    send_pages_to_promptflow,
    sqs_error_codes,
    sqs_client,
    sqs_codes,
    logger,
    upload_item_to_dynamodb,
)


def classification_workflow() -> None:
    while True:
        # NOTE LONG POLLING
        messages = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=3, WaitTimeSeconds=10
        )
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
                if sqs_codes.OCR_COMPLETED.value in body:
                    file_id = body[sqs_codes.NEW_FILE_CODE.value].get("file_id")
                    file_key = body[sqs_codes.NEW_FILE_CODE.value].get("file_key")
                    if not file_id or not file_key:
                        send_log_to_cloudwatch(
                            f"malformed SQS message for file: file_id:{file_id}, file_key:{file_key} message deleted"
                        )
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )
                        continue
                    item = get_item_from_dynamodb(
                        DYNAMODB_TABLE_NAME, key={"file_id": {"S": file_id}}
                    )
                    send_log_to_cloudwatch(
                        f"dynamodb interrogated for file: file_id:{file_id}, file_key:{file_key} before classification"
                    )
                    try:
                        classification = send_pages_to_promptflow(
                            item["Item"]["ocr_pages"]
                        )
                    except Exception as err:
                        logger.error(
                            f"an error occured during classification: {err} on file: file_id:{file_id}, file_key:{file_key} "
                        )
                        send_log_to_cloudwatch(
                            f"an error occured during classification: {err} on file: file_id:{file_id}, file_key:{file_key} "
                        )
                        send_message_to_sqs(
                            SQS_DLQ_QUEUE_URL,
                            {
                                sqs_error_codes.CLASSIFICATION_FAILED.value: {
                                    "file_id": file_id,
                                    "file_key": file_key,
                                }
                            },
                        )
                    logger.info(
                        f"classification over for file: file_id:{file_id}, file_key:{file_key}"
                    )
                    send_log_to_cloudwatch(
                        f"classification over for file: file_id:{file_id}, file_key:{file_key}"
                    )
                    for segment_index, seg in enumerate(
                        classification.get("segmentDiag", [])
                    ):
                        upload_item_to_dynamodb(
                            DYNAMODB_TABLE_NAME,
                            {
                                "file_id": {"S": file_id},
                                "sort_key": {
                                    "S": f"{seg['categorie']}#{segment_index}"
                                },
                                "category": {"S": seg["categorie"]},
                                "pages": {"L": [{"N": str(p)} for p in seg["pages"]]},
                                "segment_type": {"S": "diagnostic"},
                            },
                        )

                    for segment_index, seg in enumerate(
                        classification.get("segmentOp", [])
                    ):
                        upload_item_to_dynamodb(
                            DYNAMODB_TABLE_NAME,
                            {
                                "file_id": {"S": file_id},
                                "sort_key": {
                                    "S": f"{seg['categorie']}#{segment_index}"
                                },
                                "category": {"S": seg["categorie"]},
                                "pages": {"L": [{"N": str(p)} for p in seg["pages"]]},
                                "segment_type": {"S": "op"},
                            },
                        )
                    logger.info(
                        f"classification segments saved in dynamodb for file:  file_id:{file_id}, file_key:{file_key}"
                    )
                    send_log_to_cloudwatch(
                        f"classification segments saved in dynamodb for file:  file_id:{file_id}, file_key:{file_key}"
                    )
                    # NOTE CAN PROBABLY BE SEPARATED IN ANOTHER WORKFLOW
                    file_content = download_file_content_from_s3(file_key)
                    save_classification_segments(file_key, file_content, classification)
                    send_log_to_cloudwatch(
                        f"classification segments saved in s3 for file: file_id:{file_id}, file_key:{file_key}"
                    )
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )


classification_workflow()
