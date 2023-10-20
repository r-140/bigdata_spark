import datetime
import os

print(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

# /flights/${yyyy}/${MM}/${dd}/${HH}/_SUCCESS

outputPath = os.path.join(
        "{{ var.value.gcs_bucket }}",
        "wordcount",
        datetime.datetime.now().strftime("%Y\\%m\\%d\\%H"),
    ) + os.sep

print(outputPath)


start_date = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(hours=1), datetime.datetime.min.time()
)

print(start_date)

print(datetime.datetime.today().replace(microsecond=0, second=0, minute=0) - datetime.timedelta(hours=1))