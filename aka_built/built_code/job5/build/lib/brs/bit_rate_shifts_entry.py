from brs.bit_rate_shifts import BRS


def main(run_date):

    job_4 = BRS()
    status = job_4.__initial_method__(run_date)

    if status == True:

        print("Bit Rate Shifts Successfully Completed")
    else:
        print("Bit Rate Shifts Crashed")

def run(run_date):

    main(run_date)