#!/usr/bin/python
import json
import subprocess
import logging
import MySQLdb
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger('hadoop_hive_har.log')
Days_before_which_data_will_be_archived = 30


# Executes all the linux commands and returns the output as string
def executeCommand(command):
    try:
        logger.info("Executing : " + command)
        proc = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = proc.communicate()[0].strip()
        return output
    except Exception as e:
        logger.error(e)
        logger.error(command)


# Executes the Archival Hive Query and returns the logs (which can be processed later to find out data regarding the compression process)
def execute_archival(command):
    try:
        print "EXECUTING ==> " + command
        logger.info("Executing Archival : " + command)
        proc = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = proc.communicate()
        return error
    except Exception as e:
        logger.error(e)
        logger.error(command)


# Finds out the last timestamp of the data stored in a table/db
def getLastTimeStamp(folder):
    command = "hadoop fs -ls <FOLDER_PATH>" + folder + "  | awk -F'/' '{print $NF}'|awk -F'=' '{print $2}' | sed '/^\s*$/d' |tail -n1 "
    output = executeCommand(command)
    return output


# Finds out the first timestamp of the data stored in a table/db
def getFirstTimeStamp(folder):
    command_1 = "hadoop fs -ls <FOLDER_PATH>" + folder + "  | awk -F'/' '{print $NF}'|awk -F'=' '{print $2}' | sed '/^\s*$/d'"
    command_2 = "head -n1"
    p1 = subprocess.Popen(command_1, stdout=subprocess.PIPE, shell=True)
    p2 = subprocess.Popen(command_2, stdin=p1.stdout, stdout=subprocess.PIPE, shell=True)
    output = p2.communicate()[0].strip()
    return output


# Finds and Converts all the timestamps associated with a table based on whether they are stored/generated daily or hourly
def generate_timestamps(start_date, end_date, partition):
    dates = []
    if partition == 'daily':
        start_date = datetime.datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
        one_day = datetime.timedelta(days=1)
        while start_date <= end_date:
            start_date_str = str(start_date.strftime("%Y%m%d"))
            dates.append(start_date_str)
            start_date = start_date + one_day
    elif partition == 'hourly':
        start_date = datetime.datetime.strptime(start_date, "%Y%m%d%H")
        end_date = datetime.datetime.strptime(end_date, "%Y%m%d%H")
        one_hour = datetime.timedelta(hours=1)
        while start_date <= end_date:
            start_date_str = str(start_date.strftime("%Y%m%d%H"))
            dates.append(start_date_str)
            start_date = start_date + one_hour
    return dates


# Executes the fsck command on the folders and returns the output as a list (split by '\n')
def get_fsck_output(folder_name):
    command = "hdfs fsck " + folder_name
    output = executeCommand(command)
    output = output.split("\n")
    return output


# Formats the fsck output and returns a value based on the key from the data
def get_value_from_data(key, data):
    value = 0
    try:
        temp = data.split(key)[1].strip()
        # print temp
        value = temp.split()[0].strip()
    except Exception as e:
        logger.error(data)
        logger.error(key)
        logger.error(e)
    return value


# Acts as a wrapper function for get_fsck_output and get_value_from_data which first finds out the fsck output, then formats data to finally return a Dictionary
def get_fsck_output_list(path):
    fsck_out = get_fsck_output(path)
    temp_dict = {}
    for out in fsck_out:
        try:
            if 'Total size' in out:
                temp_dict['Total_size'] = get_value_from_data("Total size:", out)
            if 'Total dirs' in out:
                temp_dict['Total_dirs'] = get_value_from_data("Total dirs:", out)
            if 'Total files' in out:
                temp_dict['Total_files'] = get_value_from_data("Total files:", out)
            if 'Total blocks (validated)' in out:
                temp_dict['Total_blocks'] = get_value_from_data("Total blocks (validated):", out)
            if 'avg. block size' in out:
                temp_dict['avg_block_size'] = get_value_from_data("avg. block size", out)
        except Exception as e:
            logger.error(e)
    return temp_dict


# Mails the reports when Archival process is completed
def mailhandler(subject, message, recipients=None):
    msg = MIMEMultipart('alternative')
    if not recipients:
        return
    mailer = smtplib.SMTP('<SMTP_SERVER>', 25)
    mailer.login("<MAIL_ID>", "<PASSWORD>")
    msg['To'] = ", ".join(recipients)
    suffix = str(datetime.date.today().strftime("%B %d, %Y"))
    msg['Subject'] = subject + suffix
    msg['From'] = "<MAIL_SENDER>"
    part = MIMEText(message, 'html')
    msg.attach(part)
    mailer.sendmail(msg['From'], recipients, msg.as_string())


if __name__ == '__main__':

    # Connecting to the db to check, till where has the data been archived
    db = MySQLdb.connect("<DB_HOST>", "<USERNAME>", "<DB_PASSWD>", "<TABLE>")
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    tables = {}
    ##### REPLACE CLEANUP WITH HAR #####
    query = "select * from WAREHOUSE_CLEANUP_CONFIG where type = 'har' ;"
    print query
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        try:
            print row
            table = (str(row["path"])).split("/")[-2] + "/" + (str("path")).split("/")[-1]
            primary_partition = row["primary_partition"]
            secondary_partition = row["secondary_partition"]
            partition= row["partition_type"]
            days = row["cleanup_period"]
            tables[table] = {}
            tables[table]["table"] = table
            tables[table]["primary_partition"] = primary_partition
            tables[table]["secondary_partition"] = secondary_partition
            tables[table]["partition"] = partition
            if (tables[table]["secondary_partition"] == None or tables[table]["secondary_partition"] == ""):
                partition_type = "Single"
            else:
                partition_type = "Dual"
            tables[table]["partition_type"] = partition_type
            tables[table]["days"] = days

        except Exception as e:
            print "Exception: ", e
            pass

    path = '<ROOT_FOLDER_PATH>'

    # Iterating over the tables from the whitelist to perform the archiving operation
    for table in tables:

        ########## Finding out other information from the Whitelist
        days = tables[table]["days"]
        days = int(days.split("L")[0])
        no_of_days = datetime.timedelta(days=days)
        print "#### DAYS ####### " + str(days) +" ########"+ str(no_of_days)
        #############
        primary_partition = tables[table]["primary_partition"]
        partition = tables[table]["partition"]
        type = tables[table]["partition_type"]
        if type == 'Dual':
            secondary_partition = tables[table]["secondary_partition"]
            # value = tables[table]["value"]

        # Converting and Normalising timestamps to figure out what data needs to be archived
        last_date = getLastTimeStamp(table)
        if last_date == '':
            continue
        else:
            if partition == 'hourly':
                last_timestamp = datetime.datetime.strptime(last_date, "%Y%m%d%H")
            elif partition == 'daily':
                last_timestamp = datetime.datetime.strptime(last_date, "%Y%m%d")

        end_timestamp = last_timestamp - no_of_days
        if partition == 'hourly':
            end_date = str(end_timestamp.strftime("%Y%m%d%H"))
        elif partition == 'daily':
            end_date = str(end_timestamp.strftime("%Y%m%d"))

        db_name = table.split('/')[0].split('.')[0]
        table_name = table.split('/')[1]
        datasource = db_name + '.' + table_name

        query = "select timestamp from <TABLE>> where datasource = '%s' ORDER BY timestamp DESC limit 1;" % (
            datasource)
        print query
        rows_count = cursor.execute(query)
        # print "ROWS COUNT => " + str(rows_count)
        if rows_count > 0:
            start_date = cursor.fetchone()[0]
        else:
            start_date = getFirstTimeStamp(table)
            if start_date == '':
                continue

        # Normalising the data based on whether it is stored hourly/daily
        if partition == 'hourly':
            one_hour = datetime.timedelta(hours=1)
            start_timestamp = datetime.datetime.strptime(start_date, "%Y%m%d%H") + one_hour
            start_date = str(start_timestamp.strftime("%Y%m%d%H"))
        elif partition == 'daily':
            one_day = datetime.timedelta(days=1)
            start_timestamp = datetime.datetime.strptime(start_date, "%Y%m%d") + one_day
            start_date = str(start_timestamp.strftime("%Y%m%d"))

        # Case where there's no data to be archived
        if end_timestamp < start_timestamp:
            continue

        # Generating all the timestamps acc to the given data
        timestamps = generate_timestamps(start_date, end_date, partition)
        for ts in timestamps:
            # Iterating over each timestamp to process the data
            ds_path = path + table + '/' + primary_partition + '=' + ts
            command = "hadoop fs -du -s -h " + ds_path + "|awk '{print $1,$2}'"
            size = executeCommand(command)
            if size == '':
                continue

            # Finding info about the data before archival

            try:
                fsck_out_before = get_fsck_output_list(ds_path)
                print fsck_out_before
                blocks_before = fsck_out_before['Total_blocks']
                avg_size_before = fsck_out_before['avg_block_size']
                print fsck_out_before
            except Exception as ex:
                avg_size_before = "0"
                print ex

            # ARCHIVAL PROCESS
            if type == "Dual":
                print (str(datasource) + " Not Archived  <<DUAL PARTITIONED>>")
                logger.info(str(datasource) + " Not Archived  <<DUAL PARTITIONED>>")

            else:
                archiving_query = "beeline -u jdbc:hive2://<HIVE_HOST>/default -n hive -p hive -d org.apache.hive.jdbc.HiveDriver -e \"set hive.archive.enabled = true; set hive.archive.har.parentdir.settable = true; set har.partfile.size = 1099511627776; ALTER TABLE " + datasource + " ARCHIVE PARTITION ( " + folder + " = '" + ts + "'  )\""
                out = execute_archival(archiving_query)
                # print archiving_query
                print out

            # Finding info about the data after archival

            try:
                fsck_out_after = get_fsck_output_list(ds_path)
                blocks_after = fsck_out_after['Total_blocks']
                avg_size_after = fsck_out_after['avg_block_size']
                print fsck_out_after
            except Exception as ex1:
                avg_size_after = "0"
                print ex1

            # Updating the results in the Database for reference
            current_ts = datetime.datetime.strftime(datetime.datetime.now(), "%d-%m-%Y %H:%M")
            query = '''INSERT into hadoop_hive_har (datasource, timestamp, original_blocks, archived_blocks, avg_block_size_original, avg_block_size_archived, archived_date) values ('%s', '%s' ,'%s', '%s', '%s','%s','%s')''' % (
                datasource, ts, blocks_before, blocks_after, avg_size_before, avg_size_after, current_ts)
            print query
            try:
                # continue
                cursor.execute(query)
                db.commit()
            except Exception as e:
                print e
                db.rollback()
        db.close()

    db = MySQLdb.connect("<DB_HOST>", "<DB_USERNAME>", "<DB_PASSWD>", "<TABLE>")
    cursor = db.cursor()
    query = "select datasource,max(timestamp),original_blocks,archived_blocks,avg_block_size_original, avg_block_size_archived from hadoop_hive_har group by datasource;"
    rows_count = cursor.execute(query)
    if rows_count > 0:
        rows = cursor.fetchall()

    html = '<table border="2" color="black" cellspacing="0" cellpadding="7"><th>Table</th><th>Last Archived Timestamp</th><th>Original Blocks</th><th>Archived Blocks</th><th>Avg Block Size Original</th><th>Avg Block Size Archived</th>'
    row = ''

    for data in rows:
        row += '<tr>'
        for entry in data:
            row += "<td>" + entry + "</td>"
        row += '</tr>'
    html += row + '</table>'

    html += '<p>This email provides details about all the tables and their partitions which have been archived using the Hadoop Archive (HAR) functionality in Hive along with their stats.  </p>'
    html += '<p><b>PS:</b> Archiving a table using hadoop archive does NOT delete/tamper the data in any way, only the indexing pattern changes (won\'t affect the queries). This is aimed at solving small files problem which will in turn decrease the load on the Name Node</p>'
    mailhandler(subject='HAR on Hive (Archival Stats) ', message=html,
                recipients=['<EMAIL_OF_RECEIPIENTS>'])