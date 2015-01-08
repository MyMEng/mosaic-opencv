import numpy as np
import cv2
import cv
import azure

from azure.storage import QueueService
from azure.storage import BlobService
from azure.storage import TableService
from azure.storage import Entity

from base64 import b64decode
import itertools
from time import sleep
from math import ceil, floor
from pickle import loads
from random import choice

def blobToOpenCV(blob):
  arr = np.asarray(bytearray(blob), dtype=np.uint8)
  img = cv2.imdecode(arr, -1)
  return img

def blobToArray(blob):
  return loads(bytearray(blob))

# Identify most appropriate miniature
def chooseSimilar( regionChar, minises ):
  # First try by hue
  hOrg = regionChar[0]
  res = []
  for i, mini in enumerate(minises):
    res.append( ( np.abs(mini[1][0] - hOrg), i) )
  sort = sorted( res, key=lambda tup:tup[0] )
  returnVal = choice( sort[0:3] )
  # index = np.argmin( (res, i) )
  return minises[returnVal[1]][0]


blob_small = 'smallimages'
blob_analysis = 'analysis'
blob_big = 'imagecontainer'

imagesQueue = 'mosaicqueue'

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
blob_service.create_container( blob_small )
blob_service.create_container( blob_big )
blob_service.create_container( blob_analysis )

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

    # Check if analysis completed: if not continue to next message
    try:
        currentTableTask = table_service.get_entity( tableName, tablePartitionKey, tableRowKey)
    except azure.WindowsAzureMissingResourceError:
        queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
        continue

    if hasattr(currentTableTask, 'analysed'):
      if currentTableTask.analysed:
        # Do nothing analysis completed
        pass
      else:
        continue
    else:
      continue

    # Check if all miniatures are ready
    imgChildren = table_service.query_entities(tableName, "PartitionKey eq '"+tablePartitionKey+"' and parent eq '" +imgBlobName + "'" )
    miniturisation = True
    for task in imgChildren:
      if hasattr(task, 'hue') and hasattr(task, 'saturation') and hasattr(task, 'value'):
        if task.hue == -1 or task.saturation == -1 or task.value == -1:
          # continue to next message
          miniturisation = False
          break
        else:
          pass
          # OK
      else:
        # continue to next message
        miniturisation = False
        break

      if miniturisation == False:
        # continue to next message
        continue

    # Get big image analysis
    try:
      blobAnalysis = blob_service.get_blob(blob_analysis, imgBlobName)
    except azure.WindowsAzureMissingResourceError:
      queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      continue
    bigImageAnalysis = blobToArray( blobAnalysis )

    # Get big image
    try:
      blobBigImage = blob_service.get_blob(blob_big, imgBlobName)
    except azure.WindowsAzureMissingResourceError:
      queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      continue
    bigImage = blobToOpenCV(blobBigImage)
    # Resize big image

    # Get parameters
    factor = imageWidth = 25 # preprocess.cv.py
    h = factor * bigImageAnalysis.shape[1]
    w = factor * bigImageAnalysis.shape[2]

    try:
      bigImageBigger = cv2.resize(bigImage, (w,h), interpolation = cv2.INTER_AREA)
    except cv2.error as ex:
      queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      sys.stderr.write(ex)
      continue

    # Get miniatures for mosaic making
    minises = []
    for child in imgChildren:
      miniName = child.RowKey
      try:
        blobMini = blob_service.get_blob(blob_small, miniName)
      except azure.WindowsAzureMissingResourceError:
        queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
        continue
      imgMini = blobToOpenCV(blobMini)
      imgMiniHSV = cv2.cvtColor(imgMini, cv2.COLOR_BGR2HSV)
      minises.append( (imgMiniHSV, (child.hue, child.saturation, child.value)) )

    # Initialise output image
    resultImage = np.zeros( (h, w, 3 ), np.uint8 )


    for hi in np.arange(0, h, factor):
      for wi in np.arange(0, w, factor):
        hue = bigImageAnalysis[0, hi/factor, wi/factor]
        sat = bigImageAnalysis[1, hi/factor, wi/factor]
        val = bigImageAnalysis[2, hi/factor, wi/factor]
        resultImage[hi:hi+factor, wi:wi+factor, :] = chooseSimilar( (hue, sat, val), minises )
        resultImage[hi:hi+factor, wi:wi+factor, 2] = val

    # Change colour
    compiledImage = cv2.cvtColor(resultImage, cv2.COLOR_HSV2BGR)
    # Overlay
    try:
        saveImage = cv2.addWeighted( compiledImage, 0.4, bigImageBigger, 0.6, 1 )
    except cv2.error as ex:
        queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
        sys.stderr.write(ex)
        continue

    # Put to blob
    if imgBlobName[-4] == '.'  :
      mosID = imgBlobName[:-4] + "_mos" + imgBlobName[-4:]
    else :
      mosID = imgBlobName[:-5] + "_mos" + imgBlobName[-5:]
    ignore ,blobImage = cv2.imencode( '.jpg', saveImage )
    blob_service.put_block_blob_from_bytes( blob_big, mosID, str(bytearray(blobImage.flatten().tolist())) )

    # Find big image entity
    currentTableTask.mosaicId = mosID
    table_service.update_entity( tableName, tablePartitionKey, tableRowKey, currentTableTask)

    # dequeue image
    queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
