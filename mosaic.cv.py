import numpy as np
import cv2
import cv

from azure.storage import QueueService
from azure.storage import BlobService
from azure.storage import TableService
from azure.storage import Entity

from base64 import b64decode
import itertools
from time import sleep
from math import ceil, floor
from picke import load

def blobToOpenCV(blob):
    arr = np.asarray(bytearray(blob), dtype=np.uint8)
    img = cv2.imdecode(arr, -1)
    return img

def blobToArray(blob):
    return load(blob)


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
    currentTableTask = table_service.get_entity( tableName, tablePartitionKey, tableRowKey)
    if hasattr(currentTask, 'analysed'):
      if currentTask.analysed:
        # Do nothing analysis completed
        pass
      else:
        continue
    else:
      continue

    # Check if all miniatures are ready
    tasks = table_service.query_entities(tableName, "PartitionKey eq '"+tablePartitionKey+"' and parent eq '" +imgBlobName + "'" )
    miniturisation = True
    for task in tasks:
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



    # Get analysed data from blob
    # Start putting together minis
    print "Mosaic making"
    continue

    # dequeue image
    queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
