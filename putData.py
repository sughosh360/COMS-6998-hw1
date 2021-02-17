import json
import datetime
import boto3
import requests
def insert_data_into_db(data_list, db = None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb',region_name='us-east-1',
         aws_access_key_id='AKIA5O4Z4D3IMYSJMJFL',
         aws_secret_access_key='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')
    table = db.Table(table)
    for data in data_list:
        response = table.put_item(Item=data)
    return response
def write_to_file(index_list):
    f = open("index_data.json", "a")
    for idx in index_list:
        f.write((str(idx)+"\n"))
    f.close
    # loop to fetch data from yelp api using offset since we can only fetch only 50 entries at a time.
    LOCATION = 'Manhattan'
    API_KEY = 'KJeariP1SWaJu5e-pc2hpTOM_XtCal0nSHa5wDxafIQtNokfW6g1QfitpYih2nV9KUJT_lm53nVIBpkYanrfgQYCtX3xekZatawc1vLCRUrnrbogCzHfrYo2CLMoYHYx'
    LIMIT = 50
    for TERM in ['chinese', 'mexican', 'italian', 'american', 'continental', 'indian', 'thai', 'korean', 'japanese', 'asian']:
        for x in [0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]:
            OFFSET = str(x)
            url = 'https://api.yelp.com/v3/businesses/search?term=' + TERM + '&location=' + LOCATION + '&offset=' + OFFSET + '&limit=' + str(LIMIT)
            headers = {
                'Authorization': 'Bearer ' + API_KEY
            }
            response = requests.get(url, headers = headers, params = {})
            contents = response.json()['businesses']
            data_list = []
            index_list = []
            for x in contents:
                data = {
                    'business_id': str(x['id']),
                    'resto_name': x['name'],
                    'location': x['location']['address1'],
                    'coordinates': json.dumps(x['coordinates']),
                    'review': str(x['review_count']),
                    'rating': str(x['rating']),
                    'zipCode': x['location']['zip_code'],
                    'timestamp': str(datetime.datetime.now()),
                    'cuisine': TERM
                }
                index_data = {"index": {"_index": "restaurants", "_id": str(x['id'])}}
                index_data_data = {"cuisine": TERM}
                data_list.append(data)
                index_list.append(index_data)
                index_list.append(index_data_data)
            insert_data_into_db(data_list)
            write_to_file(index_list)
