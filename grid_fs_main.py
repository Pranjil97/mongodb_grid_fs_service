from flask import Flask,request, jsonify, make_response
from mongo_grid_fs_operations import Mongogrid_operations

connection_string= "mongodb://localhost:27017"
mongo=Mongogrid_operations(connection_string)

app=Flask(__name__)

@app.route("/upload/<string:filename>",methods=["POST"])
def upload(filename):
    try:
        if mongo.upload_file(filename=filename,data=request.data):
            return jsonify({"result": "success"}), 201
        else:
            return jsonify({"result": "failed"}), 304
    except Exception as e:
        print(f"Upload :: error occured :: {e}")
        return jsonify({"result": "failed"}), 500


@app.route("/delete/<string:filename>",methods=["DELETE"])
def delete(filename):
    try:
        if mongo.delete(filename=filename):
            return jsonify({"result": "success"}), 201
        else:
            return jsonify({"result": "failed"}), 304
    except Exception as e:
        print(f"delete :: error occured :: {e}")
        return jsonify({"result": "failed"}), 500


@app.route("/update/<string:filename>",methods=["PUT"])
def update(filename):
    try:
        if mongo.update(filename=filename,data=request.data):
            return jsonify({"result": "success"}), 201
        else:
            return jsonify({"result": "failed"}), 304
    except Exception as e:
        print(f"Update :: error occured :: {e}")
        return jsonify({"result": "failed"}), 500


@app.route("/read/<string:filename>",methods=["GET"])
def read(filename):
    try:
        data = mongo.read(filename=filename)
        if data!=None:
            response=make_response(data)
            response.headers["Content-Type"] = "application/octet-stream"
            return response, 200
        else:
            return jsonify({"result": "file not found"}), 404
    except Exception as e:
        print(f"list :: error occured :: {e}")
        return jsonify({"result": "server error"}), 500


@app.route("/list",methods=["GET"])
def list():
    try:
        result=mongo.list_file()
        return jsonify({"All files":result})
    except Exception as e:
        print(f"list :: error occured :: {e}")
        return jsonify({"result": "NOT WORKING"}), 500

if __name__=="__main__":
    app.run(host="0.0.0.0",port=9000,debug=True)