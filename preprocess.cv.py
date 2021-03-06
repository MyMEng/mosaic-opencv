import numpy as np
import cv2

from azure.storage import QueueService
from azure.storage import BlobService
from azure.storage import TableService
import azure

import urllib
# from os import environ
from base64 import b64decode
import itertools
from time import sleep
from math import floor

def blobToOpenCV(blob):
    arr = np.asarray(bytearray(blob), dtype=np.uint8)
    img = cv2.imdecode(arr, -1)
    return img

# read image from URL to CV format
def getImgURL( imgURL ):
  req = urllib.urlopen( imgURL )
  arr = np.asarray( bytearray(req.read()), dtype=np.uint8 )
  img = cv2.imdecode(arr, -1) # 'load it as it is'
  return img

def makeThumbnail( image, width ) :
  ## Normalise to square based on lower dimension
  imgH = image.shape[0]
  imgW = image.shape[1]

  if imgH < imgW:
    diff = imgW-imgH
    if diff%2 == 0:
      cut = (diff)/2
      crop_img = image[:, cut:imgW-cut]
    else:
      cut = (diff)/2.0
      cut = floor(cut)
      crop_img = image[:, cut+1:imgW-cut]
  elif imgH > imgW:
    diff = imgH-imgW
    if diff%2 == 0:
      cut = (diff)/2
      crop_img = image[cut:imgH-cut, :]
    else:
      cut = (diff)/2
      cut = floor(cut)
      crop_img = image[cut+1:imgH-cut, :]
  elif imgH==imgW:
    # Nothing to do
    crop_img = image

  # we need to keep in mind aspect ratio
  dim = ( width, width )
   
  # perform the actual resizing of the image
  res = cv2.resize(crop_img, dim, interpolation = cv2.INTER_AREA) # None, fx=2, fy=2
  return res

# Analyse the overall colour of image
def getCharacteristics( image ):
  # range_hist = [0, 100, -100, 100, -100, 100]
  # hist_1 = cv2.calcHist([image], [0, 1, 2], None, [20, 20, 20], range_hist)
  hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
  h = hsv[:,:,0]
  s = hsv[:,:,1]
  v = hsv[:,:,2]

  hs = list(itertools.chain.from_iterable(h.tolist()))
  ss = list(itertools.chain.from_iterable(s.tolist()))
  vs = list(itertools.chain.from_iterable(v.tolist()))

  counts = np.bincount(hs)
  hw = np.argmax(counts)

  counts = np.bincount(ss)
  sw = np.argmax(counts)

  counts = np.bincount(vs)
  vw = np.argmax(counts)

  return (int(hw), int(sw), int(vw))

blob_container = 'smallimages'
imagesQueue = 'smallimagesqueue'
imageWidth = 50

tableName = 'photos'
tablePartitionKey = 'allphotos'

# Get queue credentials
# accountName = environ["AZURE_STORAGE_ACCOUNT"]
with open ("ASA.key", "r") as myfile:
  accountName=myfile.read().replace('\n', '')
# accountKey = environ["AZURE_STORAGE_ACCESS_KEY"]
with open ("ASK.key", "r") as myfile:
  accountKey=myfile.read().replace('\n', '')

# Create blob service
blob_service = BlobService( account_name=accountName, account_key=accountKey )
blob_service.create_container( blob_container )

# Open queue with given credentials
queue_service = QueueService( account_name=accountName, account_key=accountKey )

# Open table service
table_service = TableService( account_name=accountName, account_key=accountKey )

# Repeat
while(True):

  # get images form *imagesQueue* - it is invoked by CRON
  messages = queue_service.get_messages( imagesQueue )
  if len(messages) == 0:
    sleep(15)
  for message in messages:
    # get image: image ID
    imgBlobName = b64decode( message.message_text )
    print( imgBlobName )
    tableRowKey = imgBlobName

    try:
      blob = blob_service.get_blob(blob_container, imgBlobName)
    except azure.WindowsAzureMissingResourceError:
      #queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      continue


    image = blobToOpenCV(blob) # image = getImgURL( imgURL )


    # ADDED2 #####
    if image is None:
      print "GIF attempt in pre-process"
      queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      table_service.delete_entity( tableName, tablePartitionKey, tableRowKey)
      blob_service.delete_blob( blob_container, imgBlobName)
      continue


    # process image
    image_tn = makeThumbnail( image, imageWidth )
    (hw, sw, vw) = getCharacteristics( image )

    # put thumbnail to bloob: add suffix _tn
    result ,blob_tn = cv2.imencode( '.jpg', image_tn )

    # Override
    tnID = imgBlobName
    # if imgBlobName[-4] == '.'  :
      # tnID = imgBlobName[:-4] + "_tn" + imgBlobName[-4:]
    # else :
      # tnID = imgBlobName[:-5] + "_tn" + imgBlobName[-5:]


    blob_service.put_block_blob_from_bytes( blob_container, tnID, str(bytearray(blob_tn.flatten().tolist())) )

    # {'PartitionKey': 'allPhotos', 'RowKey': 'imageName', 'thumbnail' : 'thumbnailName',
    #  'userId' : ?, 'local' : ?, 'hue' : 200, 'saturation' : 200, 'value' : 200}
    ## query for image in table to ensure existence
    currentTask = table_service.get_entity( tableName, tablePartitionKey, tableRowKey)
  

    ## send the quantities to table: save thumbnail ID & save image characteristics
    # currentTask.thumbnail = tnID
    currentTask.hue = hw
    currentTask.saturation = sw
    currentTask.value = vw
    table_service.update_entity( tableName, tablePartitionKey, tableRowKey, currentTask)

    # dequeue image
    queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
