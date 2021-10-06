from job1.data_preprocessing import parsing
from job2.Time_To_Top_Profile import TTTP
from job3.percentage_below_top_profile import PTBTP
from job4.session_startup_time import SST
from job5.bit_rate_shifts import BRS
from job6.vqem_score_device_level import VQEM_DEVICE
from job6.vqem_score_session_level import VQEM_SESSION
from job6.vqem_score_account_level import VQEM_ACCOUNT
from pytz import timezone
from datetime import datetime, timedelta

def main(run_date):

    print("Current Date of Run", run_date)
    job_1 = parsing()

    raw_df = job_1.__get_data__(run_date)
    raw_df = job_1.__filteration__(raw_df)
    status = job_1.__save__(raw_df)

    if status==True:

        print("Preprocessing Successfully Completed")
    else:
        print("Preprocessing Crashed")

    job_2 = TTTP()
    status = job_2.__initial_method__()

    if status == True:

        print("Time To Top Profile Successfully Completed")
    else:
        print("Time To Top Profile Crashed")

    job_3 = PTBTP()
    status = job_3.__initial_method__()

    if status == True:
        print("Percentage Below Top Profile Job Successfully Completed")
    else:
        print("Percentage Below Top Profile Job Crashed")

    job_4 = SST()
    status = job_4.__initial_method__()

    if status == True:
        print("Session Start Time Job Successfully Completed")
    else:
        print("Session Start Time Job Crashed")

    job_5 = BRS()
    status = job_5.__initial_method__()

    if status == True:
        print("Bitrate Shifts Job Successfully Completed")
    else:
        print("Bitrate Shifts Job Crashed")

    job_6 = VQEM_DEVICE()
    status = job_6.__initial_method__(run_date)

    if status == True:

        print("VQEM on Device Level Completed")
    else:

        print("VQEM on Device Level Crashed")

    job_7 = VQEM_SESSION()
    status = job_7.__initial_method__(run_date)

    if status == True:

        print("VQEM on Session Level Completed")
    else:
        print("VQEM on Session Level Crashed")

    job_8 = VQEM_ACCOUNT()
    status = job_8.__initial_method__(run_date)

    if status == True:

        print("VQEM on Account Level Completed")
    else:

        print("VQEM on Account Level Crashed")

if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('EST')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-1)
    main(run_date)

