'''
This file consists of all the functions that are used to interact with the database.
It also consists of the code needed to connect to the dynamoDB database using the
configurations set in config.py file.
'''

from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME

resource = resource(
    'dynamodb',
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

def create_table_movie():    
    table = resource.create_table(
        TableName = 'Movie', # Name of the table 
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'N'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

MovieTable = resource.Table('Movie')

def write_to_movie(id, title, director):
    response = MovieTable.put_item(
        Item = {
            'id'     : id,
            'title'  : title,
            'director' : director,
            'upvotes'  : 0
        }
    )
    return response

def read_from_movie(id):
    response = MovieTable.get_item(
        Key = {
            'id'     : id
        },
        AttributesToGet = [
            'title', 'director' # valid types dont throw error, 
        ]                      # Other types should be converted to python type before sending as json response
    )
    return response

def update_in_movie(id, data:dict):
    response = MovieTable.update_item(
        Key = {
            'id': id
        },
        AttributeUpdates={
            'title': {
                'Value'  : data['title'],
                'Action' : 'PUT' # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'director': {
                'Value'  : data['director'],
                'Action' : 'PUT'
            }
        },
        ReturnValues = "UPDATED_NEW"  # returns the new updated values
    )
    return response

def upvote_a_movie(id):
    response = MovieTable.update_item(
        Key = {
            'id': id
        },
        AttributeUpdates = {
            'upvotes': {
                'Value'  : 1,
                'Action' : 'ADD'
            }
        },
        ReturnValues = "UPDATED_NEW"
    )
    response['Attributes']['upvotes'] = int(response['Attributes']['upvotes'])
    return response

def modify_director_for_movie(id, director):
    response = MovieTable.update_item(
        Key = {
            'id': id
        },
        UpdateExpression = 'SET info.director = :director', #set director to new value
        #ConditionExpression = '', # execute until this condition fails # no condition
        ExpressionAttributeValues = { # Value for the variables used in the above expressions
            ':new_director': director
        },
        ReturnValues = "UPDATED_NEW"  #what to return
    )
    return response

def delete_from_movie(id):
    response = MovieTable.delete_item(
        Key = {
            'id': id
        }
    )

    return response