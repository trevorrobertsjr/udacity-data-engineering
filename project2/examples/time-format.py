import calendar
from datetime import datetime

starttime=1541110994796

timestamp = datetime.utcfromtimestamp(starttime/1000)
print("Timestamp =", starttime)
# timetuple data: Hour = time.struct_time(tm_year=2018, tm_mon=11, tm_mday=1, tm_hour=22, tm_min=23, tm_sec=14, tm_wday=3, tm_yday=305, tm_isdst=-1)
print("Hour =", timestamp.timetuple()[3])
print("Day =",timestamp.day)
print("Month =",calendar.month_name[timestamp.month])
print("Year =",timestamp.year)
print("Weekday =",calendar.day_name[timestamp.weekday()])