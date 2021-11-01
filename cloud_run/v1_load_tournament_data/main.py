import os

from flask import Flask
from modules.v1_load import download_v1, load_into_bq_v1, add_mapping_v1

app = Flask(__name__)


@app.route("/")
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Hello {}!".format(name)


# loads v1 tournament data from Numerai to BQ with an ordered row mapping
@app.route("/start_v1", methods=['POST'])
def start_v1_load():
    download_v1()
    print('func download_v1 successful')
    load_into_bq_v1()
    print('func load_into_bq_v1 successful')
    add_mapping_v1()
    print('func add_mapping_v1 successful')
    return "Did v1 download, load into bq, and added mapping"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
