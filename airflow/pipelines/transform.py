import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
import json
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import nasa_api_key



# Parse file



