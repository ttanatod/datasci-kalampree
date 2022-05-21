import base64
import numpy as np
import cv2
from google.cloud import storage
from datetime import datetime
import requests
import json

# cloud storage info
bucket_name = 'datasci-kalampree'
# path = 'radarNJ/'
# name of image in cloud storage
# img_name=['radar1.png','radar2.png','radar3.png','radar4.png','radar5.png']
img_name=[]
# size of image
# height = 586
# width = 600
# nort-east, south-west from nongjog
ne = [14.91950740666351,101.9563556674292]
sw = [12.75026910981639,99.73642387358632]
lat_dif = ne[0]-sw[0]
long_dif = ne[1]-sw[1]
# province: [lat, long]
loc_latlong = {
    "bkk" : [13.736717, 100.523186],
    "nonthaburi" : [13.859108, 100.521652],
    "nakhonpathom" : [13.814029, 100.037292],
    "chachoengsao" : [13.690419, 101.077957],
    "chonburi" : [13.361143, 100.984673],
    "samut-songkhram" : [13.41456, 100.00264],
    "samut-sakhon" : [13.54753, 100.27362],
    "samut-prakan" : [13.59934, 100.59675]
}
# color value mapping
# version no swap(BGR)
rgb2value = {
    '0,0,0'      : 0.0,
    '128,255,0'  : 5.5,
    '0,255,0'    : 10.0,
    '0,175,0'    : 15.0,
    '50,150,0'   : 20.0,
    '0,255,255'  : 25.0,
    '0,200,255'  : 30.0,
    '0,170,255'  : 35.0,
    '0,85,255'   : 40.0,
    '0,0,255'    : 45.0,
    '100,0,255'  : 50.0,
    '255,0,255'  : 55.0,
    '255,128,255': 60.0,
    '255,200,255': 65.0,
    '255,225,255': 70.0,
    '255,255,255': 75.0,
}
# url power bi
url = 'https://api.powerbi.com/beta/271d5e7b-1350-4b96-ab84-52dbda4cf40c/datasets/07d81974-5ebc-4e7b-a9f2-85795ed1236f/rows?key=u1VnUfM2K90MD9O%2BwAyj%2FTvRjamCQjBGSYnR28DHmHagNIWohmuaCesWpr1FzK7QG%2Fabl640I8ZADhLh6y1Mbw%3D%3D'
# loc-pixel
loc_pixel = {}

# download image from cloud storage
def download_blob(bucket_name, source_blob_name, destination_file_name):
    # source_blob_name is object_name
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

# swap rgb <-> bgr
def swapRGB(img):
    for r in range(len(img)):
        for c in range(len(img[r])):
           img[r][c][0], img[r][c][2] = img[r][c][2], img[r][c][0] 

#lat,long to pixel
def latlongToPixel(lat, long, width, height):
    row = (lat - sw[0])/lat_dif
    col = (long - sw[1])/long_dif
    row = height - int(row*height)
    col = int(col*width)
    return [row,col]

# change to closet value
def changeToClosetValue(v):
    dif = 1000
    new_value = v
    for k in rgb2value:
        tmp = [int(e) for e in k.split(',')]
        # print(tmp, v)
        # calculate dif
        d = abs(tmp[0]-v[0]) + abs(tmp[1]-v[1]) + abs(tmp[2]-v[2])
        if d < dif:
            new_value = tmp
            dif = d
    return new_value

# get rain value from province name
def getPixelValueFromProvince(prov, img):
    row = loc_pixel[prov][0]
    col = loc_pixel[prov][1]
    print('pixel', row, col)
    tmp = []
    # size of area to get pixel value
    i = 10
    selected_pixel = img[row-i:row+i,col-i:col+i]
    for r in selected_pixel:
        for c in r:
            new_value = changeToClosetValue(c)
            s = ','.join(str(e) for e in new_value)
            tmp.append(s)
    
    values, counts = np.unique(tmp, return_counts=True)

    # find avg from non-zero value
    sum = 0
    c = 0
    for i in range(len(values)):
        if values[i] != '0,0,0':
            sum += rgb2value[values[i]]*counts[i]
            c += counts[i]
    if c == 0: return 0
    return sum/c

def epoch_to_datetime(epo):
    date_time = datetime.fromtimestamp(epo)  
    return str(date_time)

def send_to_server(data_dict):
    url="http://34.142.140.29:8000/post"
    response = requests.request(
        method="POST",
        url=url,
        json=data_dict
    )
    print(response)

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    img_name = pubsub_message.split(',')
    print(img_name)

    # convert lat,long of each place to pixel
    
    data = []
    data2 = []
    first_time = True
    for name in img_name:
        
        # download from cloud storage
        source_blob_name = name
        tmp = name.split('/')[-1]
        print('tmp: ', tmp)
        name = '/tmp/' + tmp
        destination_file_name = name
        print("source: ", source_blob_name)
        print("destination: ", destination_file_name)
        download_blob(bucket_name, source_blob_name, destination_file_name)
        print("download success")
        # read image and swap from bgr2rgb
        img = cv2.imread(name, 1)
        height = img.shape[0]
        width = img.shape[1]

        if first_time:
            for prov, loc in loc_latlong.items():
                loc_pixel[prov] = latlongToPixel(loc[0], loc[1], width, height)
            first_time = False

        # swapRGB(img)
        print('shape', img.shape)
        # get value for each location and append to data
        for prov in loc_latlong:
            value = getPixelValueFromProvince(prov, img)
            print(value)
            lat,long = loc_latlong[prov]
            temp = name.split("/")[-1]
            temp = temp.replace(".png","")
            timestamp=epoch_to_datetime(int(temp))
            data2.append([timestamp, prov, lat, long, value])
            
    data_dict={"image_path":img_name,"data":data2}
    send_to_server(data_dict)


    print(data)

    # send data to power bi
    id = datetime.strftime(datetime.now(),"%Y%m%d%H%M%S")
    dt = datetime.strftime(datetime.now(),"%Y-%m-%dT%H:%M:%S")
    for d in data2:
        tmp = [{
            "Date" : d[0],
            "Time" : d[0],
            "Lat" : d[2],
            "Long" : d[3],
            "Rain" : d[4],
            "Name" : d[1],
            "ID" : id
            }
        ]
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.request(
            method="POST",
            url=url,
            headers=headers,
            data=json.dumps(tmp)
        )
        print(response)

#     print(pubsub_message)
