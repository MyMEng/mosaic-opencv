import numpy as np
import cv2
import cv

from azure.storage import QueueService
from azure.storage import BlobService
from azure.storage import TableService

from base64 import b64decode
import itertools
from time import sleep
from math import ceil, floor
from picke import dump

def blobToOpenCV(blob):
    arr = np.asarray(bytearray(blob), dtype=np.uint8)
    img = cv2.imdecode(arr, -1)
    return img

# Analyse the overall colour of image
def getCharacteristics( image, region, resultsHolder ):
  # Check whether region divides image and if not fill
  imgH = image.shape[0]
  imgW = image.shape[1]

  # Crop the border
  ndH = imgH % region
  ndW = imgW % region
  # if able to do it both-sided
  if ndH % 2 == 0:
    cut = ndH /2
    img = image[cut:imgH-cut, :]
  else:
    cut = floor( ndH/2 )
    img = image[(cut+1):imgH-cut, :]

  if ndW % 2 == 0:
    cut = ndW /2
    img = img[:, cut:imgW-cut]
  else:
    cut = floor( ndW/2 )
    img = img[:, (cut+1):imgW-cut]

  # Convert to HSV
  img = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

  # initialise matrix
  resultsHolder = np.zeros([3, img.shape[0], img.shape[1]])

  # Analyse quadrants
  for heigh in np.range(0, img.shape[0], region):
    for width in np.range(0, img.shape[1], region):

      h = hsv[heigh:heigh+region-1, width:width+region-1, 0]
      s = hsv[heigh:heigh+region-1, width:width+region-1, 1]
      v = hsv[heigh:heigh+region-1, width:width+region-1, 2]

      hs = list(itertools.chain.from_iterable(h.tolist()))
      ss = list(itertools.chain.from_iterable(s.tolist()))
      vs = list(itertools.chain.from_iterable(v.tolist()))

      counts = np.bincount(hs)
      hresultsHolder[0, heigh/2, width/2] = np.argmax(counts)

      counts = np.bincount(ss)
      resultsHolder[1, heigh/2, width/2] = np.argmax(counts)

      counts = np.bincount(vs)
      resultsHolder[2, heigh/2, width/2] = np.argmax(counts)

  return resultsHolder

# Constants
blob_container = 'imagecontainer'
blob_analysis = 'analysis'
imagesQueue = 'imagesqueue'

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
blob_service.create_container( blob_analysis )

# Open queue with given credentials
queue_service = QueueService( account_name=accountName, account_key=accountKey )

# Open table service
table_service = TableService( account_name=accountName, account_key=accountKey )

# Analysis results
results = None
# Regions for analysis
region = 4

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
      queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )
      continue

    image = blobToOpenCV(blob)
    # process image
    colourStructure = getCharacteristics( image, region, results )

    put_block_blob_form_bytes( blob_analysis, imgBlobName, dump(colourStructure) )

    # {'PartitionKey': 'allPhotos', 'RowKey': 'imageName', 'thumbnail' : 'thumbnailName',
    #  'userId' : ?, 'local' : ?, 'hue' : 200, 'saturation' : 200, 'value' : 200}
    ## query for image in table to ensure existence
    currentTask = table_service.get_entity( tableName, tablePartitionKey, tableRowKey)

    
  
    ## send the quantities to table: save thumbnail ID & save image characteristics
    # currentTask.thumbnail = tnID
    currentTask.analysed = True
    table_service.update_entity( tableName, tablePartitionKey, tableRowKey, currentTask)

    # dequeue image
    queue_service.delete_message( imagesQueue, message.message_id, message.pop_receipt )



# Get the mosaic image from some queue


## Divide on quadrants and analyse each quadrant ? concurrently
## Wait until table is filled with values - no NILLs - thumbnails done
## Send request for image with given statistics
## Receive thumbnail and paste it
## REPEAT

# Save the file to blob

# Send DONE notification


