from sst.session_startup_time import SST


def main(run_date):

    job_4 = SST()
    status = job_4.__initial_method__(run_date)

    if status == True:

        print("Session Startup Time Successfully Completed")
    else:
        print("Session Startup Time Crashed")

def run(run_date):

    main(run_date)