from ptbtp.percentage_below_top_profile import PTBTP


def main(run_date):

    job_3 = PTBTP()
    status = job_3.__initial_method__(run_date)

    if status == True:

        print("Percentage Below Top Profile Successfully Completed")
    else:
        print("Percentage Below Top Profile Crashed")

def run(run_date):

    main(run_date)