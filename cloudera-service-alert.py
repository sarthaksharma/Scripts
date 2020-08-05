#!/usr/bin/python
import socket
import subprocess
import time
import multiprocessing
import requests
from cm_api.api_client import ApiResource

################################## VARIABLES FOR CM MASTER, CREDS, ROLES TO MONITOR, ETC ############################
hostname = "<HOSTNAME>"
api = ApiResource(hostname, version=5, username="<USERNAME>", password="<PASSWORD>")
script="yarn_alert"
service_types = ["YARN","HIVE","HUE","HDFS","OOZIE"]
role_types = ["NODEMANAGER","RESOURCEMANAGER","JOBHISTORY","HIVESERVER2","HIVEMETASTORE","HUE_SERVER","NAMENODE","OOZIE_SERVER"]
hosts = {}
stats  = {}
group = 'service_notifier'
webhooks = {
    "service_notifier": {
        "url": "<WEBHOOK_API>",
        "name" : "Cloudera Alert Notifier"
    }
}
webhook_data = webhooks.get(group)
######################################################################################################################

################################## FUNCTION FOR SENDING WEBHOOK MESSAGES TO SLACK/FLOCK ##############################
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
######################################################################################################################

################################## FUNCTION FOR EXECUTING LINUX COMMANDS ON A SERVER #################################
def executeCommand(command):
    try:
        print "EXECUTING ==> " + command
        proc = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = proc.communicate()[0].strip()
        return output
    except Exception as e:
        print(e)
        print(command)
######################################################################################################################

####################### FUNCTION FOR CHECKING STATUS OF PREVIOUS RUN TO AVOID CONTINUOUS ALERTS ######################
def check_status(role, host, first_fail_check_min, alert_interval, alert_message):
    check_status = "cat /tmp/" + script +"_"+role+"_"+host+ "_status"
    output_status = executeCommand(check_status)
    if (str(output_status) == "1"):
        try:
            cat_timestamp = "cat /tmp/" + script +"_"+role+"_"+host+ "_time"
            prev_fail_time = executeCommand(cat_timestamp)
            cat_ptr = "cat /tmp/" + script + "_" + role + "_" + host + "_ptr"
            ptr = executeCommand(cat_ptr)
            current_time = time.time()
            diff = float(float(current_time) - float(prev_fail_time))
            if (int(diff/ 60.0) >= int(first_fail_check_min)):
                if(int(ptr)==0):
                    send_jab(webhook_data, alert_message + " \nSince " +str(int(diff/60.0))+ " minutes")
                update_ptr = "echo \"" + str(int(float(float(ptr)+1)%float(alert_interval))) + "\" > /tmp/" + script + "_" + role + "_" + host + "_ptr"
                ptr_out = executeCommand(update_ptr)
            return 1
        except Exception as e:
            print(e)
            return 1
    else:
        update_status = "echo \"1\" > /tmp/"+ script +"_"+role+"_"+host+"_status"
        output_status = executeCommand(update_status)
        update_timestamp = "echo \"" + str(int(time.time())) + "\" > /tmp/" + script +"_"+role+"_"+host+ "_time"
        output_time = executeCommand(update_timestamp)
        return 0
######################################################################################################################

####################### MAIN FUNCTION ITERATING OVER HOSTS AND ROLES ALONG WITH RELEVANT ACTIONS #####################
def main():
    for h in api.get_all_hosts():
        hosts[h.hostId] = h.hostname
    for c in api.get_all_clusters():
        for s in c.get_all_services():
            if s.type in service_types:
                roles = s.get_all_roles()
                for role in roles:
                    print role.type
                    if role.type in role_types:
                        if(str(role.healthSummary)=="BAD"):
                            msg = str(role.type) + " Status: *BAD*" + "\non Server: *" + str(hosts[role.hostRef.hostId])
                            if (str(role.type) == "RESOURCEMANAGER" or str(role.type) == "NAMENODE"):
                                out = check_status(str(role.type), str(hosts[role.hostRef.hostId]), 2.0, 3.0, msg)

                            elif(str(role.type) == "HUE_SERVER" or str(role.type) == "HIVESERVER2" or str(role.type) == "NODEMANAGER" or str(role.type) == "JOBHISTORY" or str(role.type) == "OOZIE_SERVER"):
                                out = check_status(str(role.type), str(hosts[role.hostRef.hostId]), 5.0, 5.0,msg)
                                if (int(out)==0):
                                    print "Can restart role"
                                    # s.restart_roles(role.name)
                            else:
                                print "checking for other roles"
                                out = check_status(str(role.type), str(hosts[role.hostRef.hostId]), 10.0, 10.0,msg)
                        else:
                            update_status = "echo \"0\" > /tmp/" + script +"_"+ str(role.type) +"_"+ str(hosts[role.hostRef.hostId]) +"_status"
                            output_status = executeCommand(update_status)
                            update_timestamp = "echo \"" + str(int(time.time())) + "\" > /tmp/" + script + "_" + str(role.type) + "_" + str(hosts[role.hostRef.hostId]) + "_time"
                            output_time = executeCommand(update_timestamp)
                            update_ptr = "echo \"0\" > /tmp/" + script + "_" + str(role.type) + "_" + str(hosts[role.hostRef.hostId]) + "_ptr"
                            ptr_out = executeCommand(update_ptr)
######################################################################################################################

####################### RUNNING MAIN IN A PROCESS TO AVOID POTENTIAL FORK BOMB IF RUN AS CRONJOB #####################
if __name__ == '__main__':
    p = multiprocessing.Process(target=main, name="Service Check")
    p.start()
    p.join(180)
    if p.is_alive():
        p.terminate()
        p.join()
######################################################################################################################