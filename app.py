from flask import Flask, request
import controller as dynamodb

app = Flask(__name__)


@app.route('/test')
def test():
    print("TEST SUCCESS")
    return 'TEST SUCCESS'

@app.route('/')
def root_route():
    dynamodb.create_table_movie()
    return 'Table created'

@app.route('/movie', methods=['POST'])
def add_movie():
    data = request.get_json()
    response = dynamodb.write_to_movie(data['id'], data['title'], data['director'])    
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added successfully',
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/movie/<int:id>', methods=['GET'])
def get_movie(id):
    response = dynamodb.read_from_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Item' in response):
            return { 'Item': response['Item'] }
        return { 'msg' : 'Item not found!' }
    return {
        'msg': 'Some error occured',
        'response': response
    }

@app.route('/movie/<int:id>', methods=['DELETE'])
def delete_movie(id):
    response = dynamodb.delete_from_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Deleted successfully',
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    } 

@app.route('/movie/<int:id>', methods=['PUT'])
def update_movie(id):

    data = request.get_json()
    response = dynamodb.update_in_movie(id, data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg'                : 'Updated successfully',
            'ModifiedAttributes' : response['Attributes'],
            'response'           : response['ResponseMetadata']
        }
    return {
        'msg'      : 'Some error occured',
        'response' : response
    }   

@app.route('/upvote/movie/<int:id>', methods=['POST'])
def upvote_movie(id):
    response = dynamodb.upvote_a_movieMovie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg'      : 'Upvotes the movie successfully',
            'Upvotes'    : response['Attributes']['upvotes'],
            'response' : response['ResponseMetadata']
        }
    return {
        'msg'      : 'Some error occured',
        'response' : response
    }

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)