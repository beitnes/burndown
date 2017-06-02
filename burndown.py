import csv
from collections import defaultdict
import glob
import os

ACCOUNTS_PREFIX = 'accounts'
MONITORED_PREFIX = 'monitored'
TEMPORARY_FILE = 'accounts.csv'
FIELD_NAMES = ["Id", "Email", "Name", "Status", "JoinedTimestamp",
              "JoinedMethod", "Arn", "Date"]
BURNDOWN_FILE_NAME = "burndown.csv"
def join_csv():

    # Remove the burndown file name if it exists
    try:
        os.remove(BURNDOWN_FILE_NAME)
    except OSError:
        pass

    # validate input files
    file_pairs = defaultdict(list)

    files = glob.glob('*.csv')
    if TEMPORARY_FILE in files:
        print(TEMPORARY_FILE + " needs renaming. Address the issue before proceeding.\n\n")
        exit(1)

    # Create empty burndown status
    with open(BURNDOWN_FILE_NAME, 'w+') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELD_NAMES)
        writer.writeheader()

    # Process by date
    for file in files:
        file_date = file.split(sep='.')[1]
        file_pairs[file_date].append(file)

    for file_date, file_pair in file_pairs.items():
        if len(file_pair) != 2:
            print("".join(file_pair) + " is not a pair of files. Address the issue before proceeding.\n\n")
            exit(1)
        prefixes = list()
        prefixes.append(file_pair[0].partition('.')[0])
        prefixes.append(file_pair[1].partition('.')[0])


        if ACCOUNTS_PREFIX not in prefixes:
            print(ACCOUNTS_PREFIX + " file not in [ "+ ", mv ".join(file_pair) + " ]. add file before proceeding.\n\n")
            exit(1)

        if MONITORED_PREFIX not in prefixes:
            print(MONITORED_PREFIX + " file not in file pair [ " + ", ".join(file_pair) + " ]. add file before proceeding.\n\n")
            exit(1)

        accounts = dict()
        monitored = defaultdict(bool)

        with open(file_pair[1]) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                monitored[row['Id']] = True

        with open(file_pair[0]) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                account = row['Id']
                accounts[account] = row

                accounts[account]['Date'] = file_date

                if monitored[account]:
                    accounts[account]['Status'] = 'MONITORED'

        # append to burndown status
        with open(BURNDOWN_FILE_NAME, 'a+') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELD_NAMES)
            for row in accounts.values():
                writer.writerow(row)



def main():
    join_csv()

if __name__ == "__main__":
    main()
