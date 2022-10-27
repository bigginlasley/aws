from codecs import ascii_encode
from pydoc import cli
import pytest
from consumer import *
from types import SimpleNamespace
from unittest.mock import MagicMock
import string

body = b'{"type":"update","requestId":"f2989d03-bc9c-4042-934f-552c076eb2df","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","description":"QQGRLNZY","otherAttributes":[{"name":"color","value":"red"},{"name":"size-unit","value":"cm"},{"name":"width","value":"699"},{"name":"width-unit","value":"cm"},{"name":"length","value":"649"},{"name":"price","value":"73.03"},{"name":"quantity","value":"76"},{"name":"note","value":"UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"}]}'

def test_data_prep():
    expected_owner = 'john-jones'
    data_actual, owner_actual = data_prep(body)
    expected_data = json.loads(body, object_hook= lambda x: SimpleNamespace(**x))
    assert data_actual == expected_data
    assert owner_actual == expected_owner

def test_json_prep():
    # second_body = b'{"type":"update","requestId":"f2989d03-bc9c-4042-934f-552c076eb2df","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","description":"QQGRLNZY","otherAttributes":[{"name":"color","value":"red"},{"name":"size-unit","value":"cm"},{"name":"width","value":"699"},{"name":"width-unit","value":"cm"},{"name":"length","value":"649"},{"name":"price","value":"73.03"},{"name":"quantity","value":"76"},{"name":"note","value":"UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"}]}'

    actual_serial = json_prep(body)
    expected_serialized = str({
    "type": "update",
    "requestId": "f2989d03-bc9c-4042-934f-552c076eb2df",
    "widgetId": "ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6",
    "owner": "John Jones",
    "description": "QQGRLNZY",
    "otherAttributes": [
        {
            "name": "color",
            "value": "red"
        },
        {
            "name": "size-unit",
            "value": "cm"
        },
        {
            "name": "width",
            "value": "699"
        },
        {
            "name": "width-unit",
            "value": "cm"
        },
        {
            "name": "length",
            "value": "649"
        },
        {
            "name": "price",
            "value": "73.03"
        },
        {
            "name": "quantity",
            "value": "76"
        },
        {
            "name": "note",
            "value": "UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"
        }
    ]
})
    actual_serial = actual_serial.translate({ord(c): None for c in string.whitespace})
    expected_serialized = expected_serialized.translate({ord(c): None for c in string.whitespace})
    expected_serialized = expected_serialized.replace("'", "\"")
    assert str(actual_serial) == str(expected_serialized)

def test_db_prep():
    expected_id = 'id'
    body_2 = b'{"type":"create","requestId":"6c37a3ff-26d8-4b2a-900f-2d614267e0e4","widgetId":"3750be35-9074-4879-bb43-759d38263868","owner":"Henry Hops","label":"UMNWEHK","description":"BOMKUEDKXDDIPCMMDOMTQWCOVFLDKLMZDQMGEERBTODSVWIGZVYXRXWMTQZHSHYYQUITXESHOD","otherAttributes":[{"name":"size","value":"314"},{"name":"size-unit","value":"cm"},{"name":"width","value":"637"},{"name":"price","value":"59.09"},{"name":"vendor","value":"S"}]}'
    expected_datadict = {'requestId': '6c37a3ff-26d8-4b2a-900f-2d614267e0e4', 'owner': 'Henry Hops', 'label': 'UMNWEHK', 'description': 'BOMKUEDKXDDIPCMMDOMTQWCOVFLDKLMZDQMGEERBTODSVWIGZVYXRXWMTQZHSHYYQUITXESHOD', 'id': '3750be35-9074-4879-bb43-759d38263868', 'size': '314', 'size-unit': 'cm', 'width': '637', 'price': '59.09', 'vendor': 'S'}
    actual_id, actual_datadict = db_prep(body_2)
    assert expected_datadict == actual_datadict
    assert expected_id == actual_id

def test_dest_bucket_insert():
    client = MagicMock()
    serialized_data = {
        "type": "update",
        "requestId": "f2989d03-bc9c-4042-934f-552c076eb2df",
        "widgetId": "ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6",
        "owner": "John Jones",
        "description": "QQGRLNZY",
        "otherAttributes": [
            {
                "name": "color",
                "value": "red"
            },
            {
                "name": "size-unit",
                "value": "cm"
            },
            {
                "name": "width",
                "value": "699"
            },
            {
                "name": "width-unit",
                "value": "cm"
            },
            {
                "name": "length",
                "value": "649"
            },
            {
                "name": "price",
                "value": "73.03"
            },
            {
                "name": "quantity",
                "value": "76"
            },
            {
                "name": "note",
                "value": "UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"
            }
        ]
    }
    dest_name = 'usu-cs5260-big-web'
    owner ='john-jones'
    id ='ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6'
    item_key = 1612306373799
    dest_bucket_insert(client, serialized_data, dest_name, owner, id, item_key)
    client.put_object.assert_called_with(Body=serialized_data, Bucket=dest_name, Key=f'widgets/{owner}/{id}')


# data_prep body
# body = b'{"type":"update","requestId":"f2989d03-bc9c-4042-934f-552c076eb2df","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","description":"QQGRLNZY","otherAttributes":[{"name":"color","value":"red"},{"name":"size-unit","value":"cm"},{"name":"width","value":"699"},{"name":"width-unit","value":"cm"},{"name":"length","value":"649"},{"name":"price","value":"73.03"},{"name":"quantity","value":"76"},{"name":"note","value":"UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"}]}'

# data_prep data
# expected_data = json.loads(body, object_hook= lambda x: SimpleNamespace(**x))
# namespace(description='QQGRLNZY', otherAttributes=[namespace(name='color', value='red'), namespace(name='size-unit', value='cm'), namespace(name='width', value='699'), namespace(name='width-unit', value='cm'), namespace(name='length', value='649'), namespace(name='price', value='73.03'), namespace(name='quantity', value='76'), namespace(name='note', value='UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD')], owner='John Jones', requestId='f2989d03-bc9c-4042-934f-552c076eb2df', type='update', widgetId='ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6')

# data_prep owner
# expected_owner = 'john-jones'

# json_prep body
# b'{"type":"update","requestId":"f2989d03-bc9c-4042-934f-552c076eb2df","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","description":"QQGRLNZY","otherAttributes":[{"name":"color","value":"red"},{"name":"size-unit","value":"cm"},{"name":"width","value":"699"},{"name":"width-unit","value":"cm"},{"name":"length","value":"649"},{"name":"price","value":"73.03"},{"name":"quantity","value":"76"},{"name":"note","value":"UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"}]}'

# # json_prep serialized data
# expected_serialized = {
#     "type": "update",
#     "requestId": "f2989d03-bc9c-4042-934f-552c076eb2df",
#     "widgetId": "ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6",
#     "owner": "John Jones",
#     "description": "QQGRLNZY",
#     "otherAttributes": [
#         {
#             "name": "color",
#             "value": "red"
#         },
#         {
#             "name": "size-unit",
#             "value": "cm"
#         },
#         {
#             "name": "width",
#             "value": "699"
#         },
#         {
#             "name": "width-unit",
#             "value": "cm"
#         },
#         {
#             "name": "length",
#             "value": "649"
#         },
#         {
#             "name": "price",
#             "value": "73.03"
#         },
#         {
#             "name": "quantity",
#             "value": "76"
#         },
#         {
#             "name": "note",
#             "value": "UFYFPRFURTHCJJZGHUWDMILVMLWVUOGYJYDOAPPKMXAPXGKMVHETWWFRQTBTGVWSODFDRZKIPAGZQBWKDJVQLXUHRSQKITMECYOYCAMDVIMODOEZQXPGZYXOXPEGQHIKPCOFMILFLVLSFKBITRAKXFFIHQNFUMTOIDLLKHYEMLGBQBDFXXRGJMXELQUDXUQNEZPXSSPHGBQFBBBXYDWLZZBPCIRGNQHHYMEXYLNBTHMFKDQDKNGHDPFDVCQPBNNIIGCBQJJVKXPAHJQHUFSSBWIHNWAXYOQKAWVRSHPEWBJTQKFBKCJWNBUIBPYQUQTIXKLZQLGYITHEDOWAUCUDZOAICRKAJFMPPQAGGEVIISRXGGIIRLIQRTXWVSMRD"
#         }
#     ]
# }