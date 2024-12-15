import json
from utils import (
    SQS_DLQ_QUEUE_URL,
    SQS_QUEUE_URL,
    download_file_content_from_s3,
    send_log_to_cloudwatch,
    send_message_to_sqs,
    sqs_error_codes,
    start_ocr_analysis,
    update_dynamo_table_item,
    sqs_client,
    sqs_codes,
    logger,
)


def ocr_workflow() -> None:
    while True:
        messages = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=3, WaitTimeSeconds=10
        )
        if "Messages" in messages:
            for message in messages["Messages"]:
                receipt_handle = message["ReceiptHandle"]
                body = message["Body"]
                if not body:
                    logger.info("malformed SQS message no body, deleted message")
                    send_log_to_cloudwatch(
                        "malformed SQS message no body, deleted message"
                    )
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )
                    continue
                body = json.loads(body)
                if sqs_codes.NEW_FILE_CODE.value in body:
                    file_id = body[sqs_codes.NEW_FILE_CODE.value].get("file_id")
                    file_key = body[sqs_codes.NEW_FILE_CODE.value].get("file_key")
                    if not file_id or not file_key:
                        logger.info("malformed SQS message no body, deleted message")
                        send_log_to_cloudwatch(
                            f"malformed SQS message file_id:{file_id}, file_key:{file_key} message deleted"
                        )
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )
                        continue
                    file_content = download_file_content_from_s3(file_key)
                    send_log_to_cloudwatch(
                        f"file downloaded for OCR analysis on file: file_id:{file_id}, file_key:{file_key} "
                    )
                    try:
                        pages = start_ocr_analysis(file_key, file_content)
                    except Exception as err:
                        logger.error(
                            f"an error occured during ocr: {err} on file: file_id:{file_id}, file_key:{file_key} "
                        )
                        send_log_to_cloudwatch(
                            f"an error occured during ocr: {err} on file: file_id:{file_id}, file_key:{file_key} "
                        )
                        send_message_to_sqs(
                            SQS_DLQ_QUEUE_URL,
                            {
                                sqs_error_codes.OCR_FAILED.value: {
                                    "file_id": file_id,
                                    "file_key": file_key,
                                }
                            },
                        )

                    logger.info(
                        f"OCR over file: file_id:{file_id}, file_key:{file_key}"
                    )
                    send_log_to_cloudwatch(
                        f"OCR over file: file_id:{file_id}, file_key:{file_key}"
                    )
                    update_expression = "SET ocr_pages = :ocr"
                    expression_attributes = {
                        ":ocr": {"L": [{"S": page} for page in pages]}
                    }
                    # NOTE peut etre plus intéressant de faire un put_item avec une clé de liaison entre le file_id et les pages
                                          
                    update_dynamo_table_item(
                        file_id, update_expression, expression_attributes
                    )
                    logger.info(
                        f"OCR pages added to dynamodb file item: file_id:{file_id}, file_key:{file_key}"
                    )
                    send_log_to_cloudwatch(
                        f"OCR pages added to dynamodb file item:{file_id}, file_key:{file_key}"
                    )

                    send_message_to_sqs(
                        SQS_QUEUE_URL,
                        {
                            sqs_codes.OCR_COMPLETED.value: {
                                "file_id": file_id,
                                "file_key": file_key,
                            }
                        },
                    )
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                    )


ocr_workflow()
