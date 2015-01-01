import numpy as np
import cv2
import cv

from azure.storage import QueueService
from azure.storage import BlobService

import urllib
import os import environ
from base64 import b64decode
import itertools

def blobToOpenCV(blob):
    arr = np.asarray(bytearray(blob), dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.CV_LOAD_IMAGE_COLOR)
    return img

# read image from URL to CV format
def getImgURL( imgURL ):
  req = urllib.urlopen( imgURL )
  arr = np.asarray( bytearray(req.read()), dtype=np.uint8 )
  img = cv2.imdecode(arr, -1) # 'load it as it is'
  return img

# Make thumbnails of uploaded images
def makeThumbnail( image, width ) :
  ## TODO: should I ask whether to do different sizes

  # we need to keep in mind aspect ratio
  r = (width * 1.0) / image.shape[1]
  dim = ( width, int(image.shape[0] * r) )
   
  # perform the actual resizing of the image
  res = cv2.resize(image, dim, interpolation = cv2.INTER_AREA) # None, fx=2, fy=2
  return res

# Analyse the overall colour of image
def getCharacteristics( image ):
  # range_hist = [0, 100, -100, 100, -100, 100]
  # hist_1 = cv2.calcHist([image], [0, 1, 2], None, [20, 20, 20], range_hist)
  hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
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

  return (hw, sw, vw)


blob_container = 'imagecontainer'
imagesQueue = 'imagesqueue'
imageWidth = 100

# Create blob service
blob_service = BlobService()
blob_service.create_container(blob_container)

# Get queue credentials
accountName = environ["AZURE_STORAGE_ACCOUNT"]
accountKey = environ["AZURE_STORAGE_ACCESS_KEY"]

# Open queue with given credentials
queue_service = QueueService( account_name=accountName, account_key=accountKey )

# get images form *imagesQueue* - it is invoked by CRON
messages = queue_service.get_messages( imagesQueue )
for message in messages:
  # get image
  imgBlobName = b64decode(message.message_text)
  blob = blob_service.get_blob(blob_container, imgBlobName)
  image = blobToOpenCV(blob) # image = getImgURL( imgURL )
  # process image
  image_tn = makeThumbnail( image, imageWidth )
  (hw, sw, vw) = getCharacteristics( image )

  # send the quantities to table


  # dequeue image
  queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
