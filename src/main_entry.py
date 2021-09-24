from job1.data_preprocessing import parsing
from job2.Time_To_Top_Profile import TTTP
from job3.percentage_below_top_profile import PTBTP
from job4.bit_rate_shifts import BRS
from job5.vqem_score import VQEM
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

if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('EST')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-3)
    main(run_date)

