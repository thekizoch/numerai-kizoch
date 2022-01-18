import os

from flask import Flask
from modules.trickle_in import trickle_in_fnc

app = Flask(__name__)

@app.route("/")
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Hello {}!".format(name)


# trickles in set amount
@app.route("/trickle_in", methods=['POST'])
def trickle_in():
    trickle_in_fnc()
    print('trickle_in_fnc successful')
    return "Did trickle_in_fnc"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
