#!/usr/bin/env bash

echo "Launch OCR listener..."
python3.10 ocr_workflow.py

echo "Launch CLASSIFICATION listener..."
python3.10 classification_worker.py &

echo "Lancement client CLI..."
python3.10 main.py --files documents/test_1.pdf documents/test_2.pdf "--create"