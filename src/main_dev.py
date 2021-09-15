from job1.data_exploration import parsing
from job2.Time_To_Top_Profile import TTTP
from job3.percentage_below_top_profile import PTBTP
from job4.bit_rate_shifts import BRS
from job5.vqem_score import VQEM
from pytz import timezone
from datetime import datetime, timedelta

def main(run_date):

    print("Current Date of Run", run_date)
    job_1 = parsing()
    status = job_1.__filteration__(run_date)

    if status==True:
        print("Job1 Successfully Completed")
    else:
        print("Job1 Crashed")

    # job_2 = TTTP()
    # status = job_2.__initial_method__()
    #
    # if status == True:
    #     print("Job2 Successfully Completed")
    # else:
    #     print("Job2 Crashed")
    #
    # job_3 = PTBTP()
    # status = job_3.__initial_method__()
    #
    # if status == True:
    #     print("Job3 Successfully Completed")
    # else:
    #     print("Job2 Crashed")
    #
    # job_4 = BRS()
    # status = job_4.__initial_method__()
    #
    # if status == True:
    #     print("Job4 Successfully Completed")
    # else:
    #     print("Job4 Crashed")
    #
    # job_5 = VQEM()
    # status = job_5.__initial_method__(run_date)
    #
    # if status == True:
    #     print("Job5 Successfully Completed")
    # else:
    #     print("Job5 Crashed")
    #

if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('EST')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-1)
    main(run_date)