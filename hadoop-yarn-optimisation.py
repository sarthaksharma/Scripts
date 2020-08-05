#!/usr/bin/python
import psutil
import logging
from datetime import datetime
import multiprocessing
import requests
from elasticsearch import Elasticsearch
import subprocess,re,os,json
import socket
import time
import happybase


# Initialising global variables and dictionaries
logger = logging.getLogger('container_info.log')
YARN_FLAG=-1
ES_FLAG=-1
application_dict ={}
process_dict = {}
val = 0
hostname = str(socket.gethostname())
script="yarn_stats"
batch_size = 10
host = "<HBABSE_MASTER_HOSTNAME>"
namespace = "<HBASE_NAMESPACE>"
row_count = 0
start_time = time.time()
table_name = "HBASE_TABLE"


# Setting up webhook for sending flock notifications
webhooks = {
    "yarn_notifier": {
        "url": "<WEBHOOK_API>",
        "name" : "Yarn Script Notifier"
    }
}
group = 'yarn_notifier'
webhook_data = webhooks.get(group)


# Function for sending out message to flock
def send_jab(webhook_data, message):
    data = {
        "text": message,
        "mentions": ["<USER_ID>"],
        "send_as": {
            "name": webhook_data.get("name", socket.gethostname().split(".")[0]),
            "icon": webhook_data.get("icon"),
        }
    }
    for nullkey in [key for key in data["send_as"] if not data["send_as"][key]]:
        del data["send_as"][nullkey]
    requests.post(webhook_data["url"], json=data)

# Function to establish connection to hbase table
def connect_to_hbase():
    """
    Connect to HBase server.
    This will use the host, namespace, table name, and batch size as defined in
    the global variables above.
    """
    conn = happybase.Connection(host = host,
        table_prefix = namespace,
        table_prefix_separator = ":")
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch, table

def insert_row(batch, application_dict,app_id):
    """
    Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema:
        [ app_id, app_name, user, type, queue, state, am_host, allocated_mem, allocated_cores, start_time, am_logs, tracking_url, startedTime, finishedTime]
    """
    batch.put(app_id, { "data:id": str(app_id), "data:app_name": str(application_dict[app_id]["app_name"]), "data:user": str(application_dict[app_id]["user"]),
        "data:type": str(application_dict[app_id]["type"]), "data:queue": str(application_dict[app_id]["queue"]), "data:state": str(application_dict[app_id]["state"]),
        "data:am_host": str(application_dict[app_id]["am_host"]), "data:allocated_mem": str(application_dict[app_id]["allocated_mem"]), "data:allocated_cores": str(application_dict[app_id]["allocated_cores"]),
        "data:start_time": str(application_dict[app_id]["start_time"]), "data:amContainerLogs": str(application_dict[app_id]["am_logs"]), "data:trackingUrl": str(application_dict[app_id]["trackingUrl"]), "data:startedTime": str(application_dict[app_id]["startedTime"]), "data:finishedTime": str(application_dict[app_id]["finishedTime"])})


def check_status(time_duration, alert_message):
    # If still alive check for the status of the previous run of this script
    check_status = "cat /tmp/" + script + "_status"
    output_status = executeCommand(check_status)

    # If the script failed continuously before, check for the time difference since first failure and accordingly notify on flock
    if (str(output_status) == "1"):
        cat_timestamp = "cat /tmp/" + script + "_time"
        prev_fail_time = executeCommand(cat_timestamp)
        current_time = time.time()
        if ((float(current_time) - float(prev_fail_time)) / 60.0 >= time_duration):
            send_jab(webhook_data, alert_message)
            update_timestamp = "echo " + str(time.time()) + " > /tmp/" + script + "_time"
            output_time = executeCommand(update_timestamp)
    else:
        # If script failed for the first time in a while, update timestamp and status and notify on flock
        update_status = "echo 1 > /tmp/" + script + "_status"
        output_status = executeCommand(update_status)
        update_timestamp = "echo " + str(time.time()) + " > /tmp/" + script + "_time"
        output_time = executeCommand(update_timestamp)
        send_jab(webhook_data, alert_message)


# Function for executing linux commands
def executeCommand(command):
    try:
        print "EXECUTING ==> " + command
        logger.info("Executing : " + command)
        proc = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = proc.communicate()[0].strip()
        return output
    except Exception as e:
        logger.error(e)
        logger.error(command)
        output="NULL"
        return output


# Function for executing GC related commands
def execute_gc_Command(command):
    try:
        print "EXECUTING ==> " + command
        proc = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = proc.communicate()
        output = ' '.join(output)
        return output
    except Exception as e:
        logger.error(e)
        logger.error(command)


# Parsing process logs to find out process(system) level details of yarn applications
def parse_ps_aux_output(output):
    output=output.split("\n")
    for line in output:
            logger.info("Matching REGEX for line : " + line)
            match_id = re.search(r'(yarn( )+(?P<p_id>\S*)( )+(?P<cpu_perc>\S*)( )+(?P<mem_perc>\S*)( )+(?P<vir_mem>\S*)( )+(?P<res_mem>\S*)(.)+:(..) (?P<p_type>\S*) +(.)+container\/(?P<app_id>\S*)\/(.)+ attempt_(?P<container_id>\S*))', line)
            if (match_id != None):
                logger.info("LINE MATCHED as MAP/REDUCE")
                p_type = match_id.group('p_type')
                cpu_percent = match_id.group('cpu_perc')
                virtual_mem = match_id.group('vir_mem')
                resident_mem = match_id.group('res_mem')
                process_id = match_id.group('p_id')
                application_id = match_id.group('app_id')
                container_id = match_id.group('container_id')
                container_id = "attempt_"+container_id
                container_tag = container_id.split("_")[4]+"_"+container_id.split("_")[5]
                process_dict[process_id] = {}
                if (p_type == "/bin/bash"):
                    process_type = "Container_Thread"
                    process_dict[process_id]['p_type'] = process_type
                elif (p_type == "/usr/lib/jvm/java-8-oracle/bin/java"):
                    process_type = "Process_Thread"
                    process_dict[process_id]['p_type'] = process_type
                if (container_id.split("_")[3]=="m"):
                    container_type = "Mapper"
                elif (container_id.split("_")[3]=="r"):
                    container_type = "Reducer"
                else:
                    container_type = "Unknown"
                process_dict[process_id]['cpu_perc']=cpu_percent
                process_dict[process_id]['virtual_mem']=virtual_mem
                process_dict[process_id]['resident_mem']=resident_mem
                process_dict[process_id]['container_id']=container_id
                process_dict[process_id]['app_id'] = application_id
                process_dict[process_id]['container_type'] = container_type
                process_dict[process_id]['map_red_id'] = container_tag
            else:
                match_id = re.search(r'(yarn( )+(?P<p_id>\S*)( )+(?P<cpu_perc>\S*)( )+(?P<mem_perc>\S*)( )+(?P<vir_mem>\S*)( )+(?P<res_mem>\S*)(.)+:(..) (?P<p_type>\S*) +(.)+container\/(?P<app_id>\S*)\/container(?P<container_id>\S*)(.)+org.apache.(?P<master_id>\S*))', line)
                if (match_id != None):
                    logger.info("LINE MATCHED as APPMASTER/SPARK")
                    p_type = match_id.group('p_type')
                    cpu_percent = match_id.group('cpu_perc')
                    virtual_mem = match_id.group('vir_mem')
                    resident_mem = match_id.group('res_mem')
                    process_id = match_id.group('p_id')
                    application_id = match_id.group('app_id')
                    container_id = "container"+str(match_id.group('container_id'))
                    master_id = match_id.group('master_id')
                    process_dict[process_id] = {}
                    if (master_id=="spark.deploy.yarn.ApplicationMaster"):
                        container_type = "SparkApplicationMaster"
                    elif (master_id=="hadoop.mapreduce.v2.app.MRAppMaster"):
                        container_type = "MRApplicationMaster"
                    elif (master_id=="spark.executor.CoarseGrainedExecutorBackend"):
                        container_type = "SparkExecutor"
                    elif (master_id == "spark.deploy.yarn.ExecutorLauncher"):
                        container_type = "SparkExecutorLauncher"
                    else:
                        container_type = "Unknown"
                    if (p_type == "/bin/bash"):
                        process_type = "Container_Thread"
                        process_dict[process_id]['p_type'] = process_type
                    elif (p_type == "/usr/lib/jvm/java-8-oracle/bin/java"):
                        process_type = "Process_Thread"
                        process_dict[process_id]['p_type'] = process_type
                    process_dict[process_id]['cpu_perc'] = cpu_percent
                    process_dict[process_id]['virtual_mem'] = virtual_mem
                    process_dict[process_id]['resident_mem'] = resident_mem
                    process_dict[process_id]['container_id'] = container_id
                    process_dict[process_id]['app_id'] = application_id
                    process_dict[process_id]['container_type'] = container_type
                    process_dict[process_id]['map_red_id'] = "None"
                else:
                    match_id = re.search(r'(yarn( )+(?P<p_id>\S*)( )+(?P<cpu_perc>\S*)( )+(?P<mem_perc>\S*)( )+(?P<vir_mem>\S*)( )+(?P<res_mem>\S*) (.)+:(\d\d) (?P<p_type>\S*))',line)
                    if (match_id != None):
                        logger.info("LINE MATCHED as APPMASTER/SPARK")
                        p_type = match_id.group('p_type')
                        cpu_percent = match_id.group('cpu_perc')
                        virtual_mem = match_id.group('vir_mem')
                        resident_mem = match_id.group('res_mem')
                        process_id = match_id.group('p_id')
                        if (p_type != "bash"):
                            process_dict[process_id] = {}
                            process_dict[process_id]['cpu_perc'] = cpu_percent
                            process_dict[process_id]['virtual_mem'] = virtual_mem
                            process_dict[process_id]['resident_mem'] = resident_mem
                            process_dict[process_id]['container_id'] = ""
                            process_dict[process_id]['app_id'] = ""
                            process_dict[process_id]['container_type'] = ""
                            process_dict[process_id]['map_red_id'] = "None"
                            process_dict[process_id]['p_type'] = "Unknown"


# Parsing output from the yarn api to find out meta info of the yarn applications and pushing data to HBASE
def parse_app_detail_output(output,app_id):
    match_oozie_jobs = None
    application_dict[app_id]={}
    application_dict[app_id]["app_name"] = str(output["app"]["name"])
    application_dict[app_id]["user"] = str(output["app"]["user"])
    application_dict[app_id]["queue"] = str(output["app"]["queue"])
    application_dict[app_id]["state"] = str(output["app"]["state"])
    application_dict[app_id]["am_host"] = str(output["app"]["amHostHttpAddress"])
    application_dict[app_id]["am_logs"] = str(output["app"]["amContainerLogs"])
    application_dict[app_id]["trackingUrl"] = str(output["app"]["trackingUrl"])
    application_dict[app_id]["startedTime"] = str(output["app"]["startedTime"])
    application_dict[app_id]["finishedTime"] = str(output["app"]["finishedTime"])
    app_start_time = (int(output["app"]["startedTime"])/1000)
    mb_sec_ = output["app"]["memorySeconds"]
    vcore_sec = output["app"]["vcoreSeconds"]
    app_current_time = time.time()
    allocated_mem = output["app"]["allocatedMB"]
    allocated_cores = output["app"]["allocatedVCores"]
    application_dict[app_id]["start_time"] = app_start_time
    application_dict[app_id]["allocated_mem"] = allocated_mem
    application_dict[app_id]["allocated_cores"] = allocated_cores

    if (match_oozie_jobs == None):
        match_oozie_jobs = re.search(r'(.*oozie*)', application_dict[app_id]["app_name"])
    if (output["app"]["user"] == "druid"):
        application_dict[app_id]["type"] = "DRUID"
        application_dict[app_id]["app_name"]=str((application_dict[app_id]["app_name"]).split("-index-")[0])
    elif (output["app"]["user"] == "hive"):
        application_dict[app_id]["type"] = "HIVE"
    else:
        application_dict[app_id]["type"] = output["app"]["applicationType"]
    if (output["app"]["queue"] == "root.data.camus"):
        application_dict[app_id]["type"] = "CAMUS"
    if (match_oozie_jobs != None):
        application_dict[app_id]["type"] = "OOZIE"

    print application_dict[app_id]
    # Connect to HBASE
    conn, batch, table = connect_to_hbase()
    try:
        print "Connect to HBase. To Push Data. Table name: %s, batch size: %i" % (table_name, batch_size)
        insert_row(batch, application_dict,app_id)
        batch.send()
    except Exception as e:
        print str(e)
    finally:
        conn.close()

# Parsing output of the jstat command to find out various GC values for a given process id and update process_dict
def parse_gc_output(output,pid):
    # print output
    output = output.split("\n")
    if (output[0].split()[1] == "not"):
        y_gc_cnt = 0
        y_gc_time = 0
        f_gc_cnt = 0
        f_gc_time = 0
        total_gc_time = 0
        current_heap_capacity =0
        current_heap_usage =0
    else:
        final_string = (' '.join(output[1].split()))
        # print final_string
        final_string = final_string.split()
        current_heap_capacity = float(final_string[0]) + float(final_string[1]) + float(final_string[4]) + float(final_string[6])
        current_heap_usage = float(final_string[2]) + float(final_string[3]) + float(final_string[5]) + float(final_string[7])
        y_gc_cnt = float(final_string[12])
        y_gc_time = float(final_string[13])
        f_gc_cnt = float(final_string[14])
        f_gc_time = float(final_string[15])
        total_gc_time = float(final_string[16])
    process_dict[pid]["current_heap_capacity"] = current_heap_capacity
    process_dict[pid]["current_heap_usage"] = current_heap_usage
    process_dict[pid]["young_gc_cnt"] = y_gc_cnt
    process_dict[pid]["young_gc_time"] = y_gc_time
    process_dict[pid]["final_gc_cnt"] = f_gc_cnt
    process_dict[pid]["final_gc_time"] = f_gc_time
    process_dict[pid]["total_gc_time"] = total_gc_time



def main():
    global YARN_FLAG
    global ES_FLAG
    start_time = time.time()

    # Find out yarn related linux processes (except nodemanager) and populate process_dict with details about each process
    ps_aux_command = "ps aux | grep ^yarn | grep -v nodemanager"
    ps_aux_output = executeCommand(ps_aux_command)
    parse_ps_aux_output(ps_aux_output)
    final_dict = {}

    # For a given process id find out GC values and yarn-app related metrics like user,app_name, etc
    for pid in process_dict:
        print "\n\n$$$$$$$$$$$$$ PID : " + str(pid) + " $$$$$$$$$$$$$$$$$$$"
        if(psutil.pid_exists(int(pid)) == True):
            logger.info("ADDING PID " + str(pid) +" FOR CONTAINER " + str(process_dict[pid]['container_id']) + " FOR APPLICATION " +str(process_dict[pid]['app_id']))
            p = psutil.Process(int(pid))
            try:
                # GC related metrics for a given java process and set default value to zero for other processes
                if(process_dict[pid]['p_type'] == "Process_Thread"):
                    gc_command = "sudo jstat -gc " + str(pid)
                    gc_output = execute_gc_Command(gc_command)
                    parse_gc_output(gc_output,pid)
                else:
                    process_dict[pid]["current_heap_usage"] = process_dict[pid]["current_heap_capacity"] = process_dict[pid]["young_gc_cnt"] = process_dict[pid]["young_gc_time"] = process_dict[pid]["final_gc_cnt"] = process_dict[pid]["final_gc_time"] = process_dict[pid]["total_gc_time"] = 0

                # Using psutil to find out system level details for a given pid
                app_id = process_dict[pid]['app_id']
                total_cpu_percent = float(process_dict[pid]['cpu_perc'])
                total_cpu_system = p.cpu_times().system
                total_cpu_user = p.cpu_times().user
                total_mem_resident = p.memory_info().rss
                total_mem_virtual = p.memory_info().vms
                total_mem_shared = p.memory_info().shared
                total_mem_MB = psutil.virtual_memory().total/(1024*1024)
                no_of_threads = p.num_threads()
                no_of_ctx_switches = p.num_ctx_switches()
                no_of_fd = p.num_fds()

                # Connect to HBASE to check for application data
                conn, batch, table = connect_to_hbase()
                try:
                    print "Connect to HBase. To Fetch Data. Table name: %s, batch size: %i" % (table_name, batch_size)
                    row_as_dict = dict(table.rows([app_id]))
                except Exception as e:
                    print "ERROR FETCHING DATA"
                    print str(e)
                finally:
                    conn.close()

                print "$$$$$$$$$$$$$ APP ID : "+str(app_id)+" $$$$$$$$$$$$$$$$$$$"
                # Check if the application detail is already present, else use yarn api to fetch yarn-app details and push to HBASE
                if(app_id in row_as_dict):
                    application_dict[app_id] = {}
                    application_dict[app_id]["app_name"] = row_as_dict[app_id]['data:app_name']
                    application_dict[app_id]["user"] = row_as_dict[app_id]['data:user']
                    application_dict[app_id]["queue"] = row_as_dict[app_id]['data:queue']
                    application_dict[app_id]["state"] = row_as_dict[app_id]['data:state']
                    application_dict[app_id]["am_host"] = row_as_dict[app_id]['data:am_host']
                    application_dict[app_id]["allocated_mem"] = int(row_as_dict[app_id]['data:allocated_mem'])
                    application_dict[app_id]["allocated_cores"] = int(row_as_dict[app_id]['data:allocated_cores'])
                    application_dict[app_id]["start_time"] = float(row_as_dict[app_id]['data:start_time'])
                    application_dict[app_id]["type"] = row_as_dict[app_id]['data:type']
                elif (app_id==""):
                    continue
                else:
                    YARN_FLAG=0
                    url = "<YARN_API_FOR_APPLICATIONS>" + str(app_id)
                    print url
                    response = requests.get(url)
                    json_app_data = json.loads(response.text)
                    parse_app_detail_output(json_app_data, process_dict[pid]['app_id'])
                    YARN_FLAG=1
                if (int(total_cpu_percent/100) == 0):
                    total_cores_used = 1
                else:
                    total_cores_used = int(total_cpu_percent/100)

                # Set all values for a given pid, gathered from various sources into a single final_dict
                final_dict[pid]={}
                final_dict[pid]['service'] = "YARN"
                final_dict[pid]['hostname'] = socket.gethostname()
                final_dict[pid]['app_id'] = process_dict[pid]['app_id']
                final_dict[pid]['container_type'] = process_dict[pid]['container_type']
                final_dict[pid]['container_id'] = process_dict[pid]['container_id']
                final_dict[pid]['process_id'] = pid
                final_dict[pid]['process_type'] = process_dict[pid]['p_type']
                final_dict[pid]['map_red_id'] = process_dict[pid]['map_red_id']
                final_dict[pid]['app_name'] = application_dict[app_id]["app_name"]
                final_dict[pid]['app_type'] = application_dict[app_id]['type']
                final_dict[pid]['user'] = application_dict[app_id]['user']
                final_dict[pid]['queue'] = application_dict[app_id]['queue']
                final_dict[pid]['state'] = application_dict[app_id]['state']
                final_dict[pid]['am_host'] = application_dict[app_id]['am_host']
                final_dict[pid]['allocated_mem_MB'] = int(application_dict[app_id]["allocated_mem"])
                final_dict[pid]['allocated_vcores'] = int(application_dict[app_id]["allocated_cores"])
                final_dict[pid]['time_taken'] = time.time() - application_dict[app_id]["start_time"]
                final_dict[pid]['cpu_percent'] = total_cpu_percent
                final_dict[pid]['cpu_cores_used'] = total_cores_used
                final_dict[pid]['cpu_system_time'] = total_cpu_system
                final_dict[pid]['cpu_user_time'] = total_cpu_user
                final_dict[pid]['total_mem_MB'] = total_mem_MB
                final_dict[pid]['current_heap_capacity_MB'] = process_dict[pid]["current_heap_capacity"]/(1024)
                final_dict[pid]['off_heap_memory_MB'] = (total_mem_resident/(1024) - process_dict[pid]["current_heap_usage"])/(1024)
                if (final_dict[pid]['off_heap_memory_MB']<0):
                    final_dict[pid]['off_heap_memory_MB'] = 0
                final_dict[pid]['current_heap_usage_MB'] = process_dict[pid]["current_heap_usage"]/(1024)
                final_dict[pid]['young_gc_cnt'] = process_dict[pid]["young_gc_cnt"]
                final_dict[pid]['young_gc_time'] = process_dict[pid]["young_gc_time"]
                final_dict[pid]['final_gc_cnt'] = process_dict[pid]["final_gc_cnt"]
                final_dict[pid]['final_gc_time'] = process_dict[pid]["final_gc_time"]
                final_dict[pid]['total_gc_time'] = process_dict[pid]["total_gc_time"]
                final_dict[pid]['mem_resident_MB'] = total_mem_resident/(1024*1024)
                final_dict[pid]['mem_virtual_MB'] = total_mem_virtual/(1024*1024)
                final_dict[pid]['mem_shared_MB'] = total_mem_shared/(1024*1024)
                final_dict[pid]['process_details'] = ""
                final_dict[pid]['timestamp'] = datetime.now()
                final_dict[pid]['thread_count'] = no_of_threads
                final_dict[pid]['voluntary_context_switches'] = no_of_ctx_switches[0]
                final_dict[pid]['involuntary_context_switches'] = no_of_ctx_switches[1]
                final_dict[pid]['file_desc_count'] = no_of_fd
            except Exception as e :
                print "Process killed/finished while processing its info"
                print str(e)

    # Connect to ElasticSearch endpoint and configure index details
    print "\n\n"
    ES_FLAG=0
    es = Elasticsearch(['<ES_HOSTNAME>'])
    doc_type_name = 'system-metrics'
    current_time = datetime.now()
    day = current_time.strftime("%Y.%m.%d")
    index_name = "<ES_INDEX_NAME>-" + day
    if current_time.hour == 0:
        es.indices.create(index=index_name, ignore=400)

    # Push each document to ES iteratively
    for keys in final_dict:
        print final_dict[keys]
        try:
            es.index(index=index_name, doc_type=doc_type_name, body=final_dict[keys], request_timeout=300)
            ES_FLAG = 1
        except Exception as e:
            print "Exception: ", e
            pass

    final_time = time.time()
    print("TOTAL TIME TAKEN FOR " + str(len(final_dict)) + " PROCESSES --- %s seconds ---" % (final_time - start_time))

    # Update the status of current run:     0 -> success     1 -> failure
    update_status = "echo 0 > /tmp/" + script + "_status"
    output_status = executeCommand(update_status)


if __name__ == '__main__':
    p = multiprocessing.Process(target=main, name="Yarn_Process_Stats")
    p.start()
    p.join(1000)
    # Check if the main process is still alive after the given time period
    if p.is_alive():
        message = "*yarn_process_statsy* Script Error \nServer: *" + str(hostname) + "*"
        check_status(15, message)
        # Finally terminate main process forcefully to avoid multiple script instances
        p.terminate()
        p.join()