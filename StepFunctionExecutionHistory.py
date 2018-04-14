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
    next_token_val = None
    results = []

    sf_client = boto3.client('stepfunctions')

    while True:
        if next_token_val != None:
            response = sf_client.list_executions(
                stateMachineArn=stateMachineArn,
                maxResults=1000,
                nextToken=next_token_val)
        else:
            response = sf_client.list_executions(
                stateMachineArn=stateMachineArn,
                maxResults=1000)

        for execution in response["executions"]:

            start_dt = execution["startDate"]
            stop_dt = execution["stopDate"]

            start_dt = datetime.datetime(year=start_dt.year, month=start_dt.month, day=start_dt.day,
                                         hour=start_dt.hour, minute=start_dt.minute, second=start_dt.second)

            stop_dt = datetime.datetime(year=stop_dt.year, month=stop_dt.month, day=stop_dt.day,
                                        hour=stop_dt.hour, minute=stop_dt.minute, second=stop_dt.second)

            # if execution start or stop datetime is within the filter range then get_exec_history and add to log
            if (filter_start_datetime <= start_dt <= filter_end_datetime) \
                    or \
                    (filter_start_datetime <= stop_dt <= filter_end_datetime):
                exec_response = sf_client.get_execution_history(executionArn=execution["executionArn"])
                execution["events"] = exec_response["events"]
                results.append(execution)

        if "nextToken" in response:
            next_token_val = response["nextToken"]
        else:
            break

        break

    return results


# ----------------------------------------------------------------------------------------------------------------------
def write_logs(data, file_name):
    with open(file_name, "wt") as f:
        f.write(data)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    # NOTE: Update vars before executing
    # ***************************************************************************************************
    stateMachineArn = 'arn:aws:states:ap-southeast-2:1234567890:stateMachine:LambdaStateMachine'

    filter_start_datetime = datetime.datetime(year=2018, month=4, day=13, hour=0, minute=0, second=0)
    filter_end_datetime = datetime.datetime(year=2018, month=4, day=13, hour=23, minute=59, second=59)

    # ***************************************************************************************************

    if not valid_datetime_filter(filter_start_datetime, filter_end_datetime):
        print("Start datetime must be lesser or qual than End datetime")
    else:
        print("Exporting logs between '{}' and '{}'".format(
            filter_start_datetime.strftime(g_timeformat),
            filter_end_datetime.strftime(g_timeformat)))

        logs = getStepFunctionLogs()
        print("log entries: {}".format(len(logs)))

        print("Formatting JSON")
        data = json.dumps(logs, default=myconverter, indent=4)

        print("writing to log file: '{}'".format(g_export_file))
        write_logs(data, g_export_file)

        print("complete")
