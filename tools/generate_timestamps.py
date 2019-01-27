import os
from datetime import datetime, timezone, timedelta

now = datetime.utcnow()

date = datetime(2017, 1, 1, tzinfo=timezone.utc)

timestamps = [];

num_days_to_output = 365 * 20 # 20 years

# determine the UTC timestamp for the start and end of each day
for x in range (num_days_to_output):
	start_of_day = round(date.timestamp());

	date_name = date.strftime("%Y-%m-%d")

	date += timedelta(days=1)

	end_of_day = round(date.timestamp() - 1);

	timestamps.append(str(start_of_day) + "," + str(end_of_day) + "," + str(date_name));

# delete previous logs if found
fname = 'timestamps.csv'
if (os.path.exists(fname)):
    os.remove(fname);

# write new logs file
with open(fname, 'a') as the_file:
    the_file.write('\n'.join(timestamps));

print("wrote timestamps to " + fname)