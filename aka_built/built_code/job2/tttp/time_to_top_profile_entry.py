from tttp.time_to_top_profile import TTTP


def main(run_date):

    job_2 = TTTP()
    status = job_2.__initial_method__(run_date)

    if status == True:

        print("Time To Top Profile Successfully Completed")
    else:
        print("Time To Top Profile Crashed")

def run(run_date):

    main(run_date)