import os as os
import time as time
import pandas as pd
import numpy as np
import pyodbc as pyodbc
import glob as glob


# create a list of all available pyodbc drivers and select the most recent driver
def SelectBestDriver():
    driver_names = [i for i in pyodbc.drivers() if i.endswith(' for SQL Server')]
    driver_name = driver_names[0]
    return driver_name


# Connect to Survey_Sample_A19 using the most latest driver
class Driver(object):

    def __init__(self):
        try:
            self.conn = pyodbc.connect('Driver={' + str(SelectBestDriver()) + '};'
                                                                              'Server=LAPTOP-OMESV5NK;'
                                                                              'Database=Survey_Sample_A19;'
                                                                              'Trusted_Connection=yes;')

            self.cursor = self.conn.cursor()
        except:
            print("Connection Failed")

    def query_str(self):
        # Get the SurveyIds
        allSurveyID = self.cursor.execute("""SELECT SurveyId FROM dbo.[Survey]""").fetchall()

        # Instantiate lists to use in outer query
        Answers = []
        Questions = []
        Survey = []

        # Loop through the surveyIds and return the survey structures with whether questions are in the survey
        for a in allSurveyID:
            # append SurveyIds to above list
            SurveyId = a[0]
            Survey.append(SurveyId)
            currentSurveyIDs = self.cursor.execute("""

                                    SELECT *
                               FROM
                               (
                                  SELECT
                                     SurveyId,
                                     QuestionId,
                                     1 as InSurvey
                                  FROM
                                     SurveyStructure
                                  WHERE
                                     SurveyId = ?
                                  UNION
                                  SELECT 
                                     ? as SurveyId,
                                     Q.QuestionId,
                                     0 as InSurvey
                                  FROM
                                     Question as Q
                                  WHERE NOT EXISTS
                                  (
                                     SELECT *
                                     FROM SurveyStructure as S
                                     WHERE S.SurveyId = ? AND S.QuestionId = Q.QuestionId
                                  )
                               ) as t
                               ORDER BY QuestionId
                                """, SurveyId, SurveyId, SurveyId).fetchall()

            for b in currentSurveyIDs:
                survey = (b[0])
                questions = (b[1])
                InSurvey = (b[2])

                # If the question is in the survey, proceed with strQueryTemplateForAnswerColumn
                questionIds = []
                surveyIds = []
                Questions.append(questions)
                if InSurvey == 1:
                    questionIds.append(questions)
                    surveyIds.append(survey)

                    # Put questionIds list and surveyIds list in one list to iterate through
                    AnswerCol = list(zip(surveyIds, questionIds))
                    for _ in AnswerCol:
                        strQueryTemplateForAnswerColumn = ",COALESCE ((SELECT a.Answer_Value FROM dbo.Answer a " \
                                                          "WHERE " \
                                                          "a.UserId = u.UserId AND a.SurveyId = " + str(
                            surveyIds[0]) + " AND " \
                                            "a.QuestionId = " + str(
                            questionIds[0]) + "),-1) AS ANS_Q" + str(questionIds[0])

                        # append this result to Answers list to use in outer query
                        Answers.append(strQueryTemplateForAnswerColumn)

                # If the question is in the survey, proceed with strQueryTemplateForNullColumn
                elif InSurvey == 0:
                    Nulls = [questions]
                    strQueryTemplateForNullColumn = ",NULL AS ANS_Q" + str(Nulls[0])

                    # append this result to Answers list to use in outer query
                    Answers.append(strQueryTemplateForNullColumn)

        # Get the number of questions there are in the survey structure
        N = len(np.unique(Questions))
        # Group the Answers list by the number of questions there are in the survey, resulting in a list of lists of
        # answer queries for each survey
        subList = [Answers[n:n + N] for n in range(0, len(Answers), N)]
        # add the surveyIds to the beginning of each list of answer queries to use in the final query
        subList2 = tuple(zip(Survey, subList))

        # the final list holds a list of complete queries for each survey
        final = []
        for c in subList2:
            strQueryTemplateOuterUnionQuery = "UNION " + "SELECT UserId, " + str(
                (c[:1][0])) + " as SurveyId " + str(
                ''.join(map(str, (c[1:][0])))) + " FROM [User] as u WHERE EXISTS (SELECT * FROM Answer " \
                                                 "as a WHERE u.UserId = a.UserId AND a.SurveyId = " + str(
                (c[:1][0])) + ")"

            final.append(strQueryTemplateOuterUnionQuery)
        # get rid of extra punctuation from appending to a list
        final2 = ''.join(map(str, final))
        # delete first UNION string
        final3 = final2[5:]
        return final3

    # This function creates a pandas dataframe from the SurveyStructure table and replaces potential nan values with 0
    # for later comparison if need be.
    def generate_df(self):
        df = pd.read_sql_query("SELECT * FROM dbo.[SurveyStructure]", self.conn)
        df2 = df.replace({np.nan: float(0)})
        return df2

    # This function uses the above query to create or alter the view, vw_AllSurveyData, and raises an exception if
    # the query generated is erroneous
    def create_alterView(self):
        query = Driver()
        try:
            self.cursor.execute("CREATE OR ALTER VIEW vw_AllSurveyData AS " + query.query_str())
            self.conn.commit()
        except Exception:
            print("Failed to update vw_AllSurveyData. Check query")


# This function writes the SurveyStructure df to a csv and attaches a date/time to the file title
def WriteToCSV():
    try:
        placeholder = Driver()
        df3 = placeholder.generate_df()
        df4 = df3.replace({0: "NULL"})
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M%S', t)
        df5 = df4.to_csv('Survey_csv/SurveyStructure_data_' + timestamp + ".csv",
                         encoding="utf-8", index=False, header=True)
        return df5
    except Exception:
        print("Writing current Survey Structure to CSV failed")


# This function first checks the folder where the SurveyStructure csv files are stores. If it is the first run and
# the folder is empty, the function calls WriteToCSV() and create_alterView(). If the csv folder is not empty, the
# function reads the latest csv.
def CheckDir():
    if len(os.listdir('Survey_csv')) == 0:
        WriteToCSV()
        placeholder = Driver()
        placeholder.create_alterView()
    else:
        try:
            latest_in_dir = pd.read_csv(max(glob.glob('Survey_csv/*.csv'), key=os.path.getmtime))
        except Exception:
            print("Reading last Survey Structure failed")

        # After reading the latest csv from the directory, this function first replaces possible nan values with 0
        # for comparison. The function checks if the values from the generated dataframe of the survey structures
        # matches the values from the latest dataframe of the survey structures and returns an array of boolean
        # values. If the sum of False values == 0, meaning there have been changes since the last view, the function
        # prints "No Change". If there are differences between the last view and the dataframe generated,
        # the function calls WriteToCSV() and create_alterView() and prints "Check Directory"

        def Compare():
            try:
                # read the latest dataframe and replace potential nan with 0
                latest_in_dir.fillna(0, inplace=True)
                # call the driver class to access generate_df() method to compare with the latest dataframe, returning
                # array of boolean values.
                placeholder = Driver()
                test = (placeholder.generate_df().values == latest_in_dir.values)
                # Sets condition_total as the total number of False values (differences between the two dataframes)
                condition_total = np.size(test) - np.count_nonzero(test)
                # Print No Change if there are no differences or write the altered survey structure to a new CSV and
                # update the view
                if condition_total == 0:
                    print("No Change")
                else:
                    WriteToCSV()
                    placeholder.create_alterView()
                    print("Check Directory for updated Survey Structure")
                return ""
            except Exception:
                print("Comparison of current and last Survey Structure failed.")

        # return Compare() object
        return Compare()


print(CheckDir())
