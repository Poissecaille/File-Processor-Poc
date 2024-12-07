import random
import time

from flask import Flask, jsonify, abort, request
from pypdf import PdfReader

app = Flask(__name__)

categories = ["etat_hypothecaire", "titre_propriete", "diagnostic", "autre"]


@app.route("/ocr", methods=["POST"])
def ocr():
    # Randomly sleeping for 0 to 3 seconds
    time.sleep(random.uniform(0, 2))

    # Randomly throwing a 500 error 1 in 100 calls
    if random.randint(1, 100) == 1:
        abort(500)

    # Read the text of the document
    pdf = PdfReader(request.files["pdf"])
    pages = []
    for page in pdf.pages:
        pages.append(page.extract_text())

    return jsonify({"pages": pages})


@app.route("/score", methods=["POST"])
def score():
    # Randomly sleeping for 0 to 3 seconds
    time.sleep(random.uniform(0, 2))

    # Randomly throwing a 500 error 1 in 100 calls
    if random.randint(1, 100) == 1:
        abort(500)

    # Random classification
    pages = request.json["pages"]
    if len(pages) == 1:
        categorie = random.choice(categories)
        if categorie == "diagnostic":
            segments = {"segmentDiag": [[0, 0]], "segmentOp": []}
        else:
            segments = {"segmentDiag": [], "segmentOp": [[0, 0]]}

    index = 0
    segments = {"segmentDiag": [], "segmentOp": []}
    while index < len(pages):
        n = random.randint(1, 3)
        categorie = random.choice(categories)
        if categorie == "diagnostic":
            segments["segmentDiag"].append(
                {
                    "pages": [index, min(index + n, len(pages) - 1)],
                    "categorie": categorie,
                }
            )
        elif categorie in ["etat_hypothecaire", "titre_propriete"]:
            segments["segmentOp"].append(
                {
                    "pages": [index, min(index + n, len(pages) - 1)],
                    "categorie": categorie,
                }
            )
        index += n

    return jsonify(segments)


if __name__ == "__main__":
    app.run(port=8000, debug=True)
