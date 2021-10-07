from brs.bit_rate_shifts import BRS


def main():

    job_4 = BRS()
    status = job_4.__initial_method__()

    if status == True:

        print("Bit Rate Shifts Successfully Completed")
    else:
        print("Bit Rate Shifts Crashed")

def run():

    main()