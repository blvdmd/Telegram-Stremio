from Backend.helper.database import Database
from Backend.helper.unsorted_collection import UnsortedCollection
from time import time
from datetime import datetime
import pytz

timezone = pytz.timezone("Asia/Kolkata")
now = datetime.now(timezone)
StartTime = time()


USE_DEFAULT_ID: str = None
db = Database()
unsorted_collection = UnsortedCollection(db)  # Manages 'unsorted' collection in same DB

__version__ = "1.5.0"
