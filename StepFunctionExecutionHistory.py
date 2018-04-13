import boto3
import json
import datetime
import os

g_export_file = os.path.join(os.getcwd(), 'results.json')
g_timeformat = '%d %b %Y %H:%M:%S'

# ----------------------------------------------------------------------------------------------------------------------
def valid_datetime_filter(start_datetime, end_datetime):
    return isinstance(start_datetime, datetime.datetime) and \
            isinstance(end_datetime, datetime.datetime) and \
            start_datetime <= end_datetime

# ----------------------------------------------------------------------------------------------------------------------
def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

# ----------------------------------------------------------------------------------------------------------------------
def getStepFunctionLogs():
    nextTokenVal = None
    results = []
    exit_loop = False

    sf_client = boto3.client('stepfunctions')

    while True:
        if nextTokenVal != None:
            response = sf_client.list_executions(
                stateMachineArn=stateMachineArn,
                maxResults=1000,
                nextToken=nextTokenVal)
        else:
            response = sf_client.list_executions(
                stateMachineArn=stateMachineArn,
                maxResults=1000)

        for execution in response["executions"]:

            #get the datetime from execution["stopDate"] which is in the format "2018-02-14 14:07:27.777000+11:00"
            #strip the micro seconds and timezone
            step_exec_date = str(execution["stopDate"]).split(".")[0]
            exec_stop_datetime = datetime.datetime.strptime(step_exec_date, '%Y-%m-%d %H:%M:%S')

            if exec_stop_datetime >= filter_start_datetime and \
                            exec_stop_datetime <= filter_end_datetime:
                exec_response = sf_client.get_execution_history(executionArn=execution["executionArn"])
                execution["events"] = exec_response["events"]
                results.append(execution)

            elif exec_stop_datetime >= filter_end_datetime:
                exit_loop = True
                break

            else:
                pass

        if exit_loop:
            break

        if "nextToken" in response:
            nextTokenVal = response["nextToken"]
        else:
            break

    return results

# ----------------------------------------------------------------------------------------------------------------------
def write_logs(data, file_name):
    with open(file_name,"wt") as f:
        f.write(data)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    # NOTE: Update vars before executing
    # ***************************************************************************************************
    stateMachineArn = 'arn:aws:states:ap-southeast-2:1234567890:stateMachine:LambdaStateMachine'

    filter_start_datetime = datetime.datetime(year=2018, month=3, day=5, hour=0, minute=0, second=0)
    filter_end_datetime = datetime.datetime(year=2018, month=3, day=5, hour=23, minute=59, second=59)

    # ***************************************************************************************************

    if not valid_datetime_filter(filter_start_datetime,filter_end_datetime):
        print("Start datetime must be lesser or qual than End datetime")
    else:
        print("Exporting logs between '{}' and '{}'".format(
                filter_start_datetime.strftime(g_timeformat),
                filter_end_datetime.strftime(g_timeformat)))

        logs = getStepFunctionLogs()
        print("logs entries: {}".format(len(logs)))

        print("Formatting JSON")
        data = json.dumps(logs, default=myconverter, indent=4)

        print("writing to log file: '{}'".format(g_export_file))
        write_logs(data, g_export_file)

        print("complete")