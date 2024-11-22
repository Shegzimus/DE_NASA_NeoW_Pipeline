import os
import sys
from datetime import datetime, timedelta



start_of_week = datetime.strptime(START_DATE, "%Y-%m-%d").strftime("%Y%m%d")
end_of_week = datetime.strptime(END_DATE, "%Y-%m-%d").strftime("%Y%m%d")

file_postfix = datetime.now().strftime("%Y%m%d")
