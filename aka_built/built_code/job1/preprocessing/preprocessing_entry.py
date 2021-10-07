from preprocessing.data_preprocessing import parsing


def main(run_date):

        print("Current Date of Run", run_date)
        job_1 = parsing()
        raw_df = job_1.__get_data__(run_date)
        raw_df = job_1.__filteration__(raw_df)
        status = job_1.__save__(raw_df)

        if status == True:

            print("Preprocessing Successfully Completed")
        else:
            print("Preprocessing Crashed")

def run(run_date):

    main(run_date)