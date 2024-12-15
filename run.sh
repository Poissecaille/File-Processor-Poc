#!/usr/bin/env bash


echo "Lancement client CLI..."
python3.10 main.py --files documents/test_1.pdf documents/test_2.pdf "--create"

echo "Starting local API..."
python3.10 api.py & 

echo "Launch OCR listener..."
python3.10 ocr_worker.py &

echo "Launch CLASSIFICATION listener..."
python3.10 classification_worker.py &

# Attendre la fin des processus en arrière-plan
wait
