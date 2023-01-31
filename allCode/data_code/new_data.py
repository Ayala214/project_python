import random
from datetime import date, timedelta
from functools import partial
from time import sleep, time
from calendar import monthrange
import os
import pandas as pd
import urllib3
from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq
from random import randrange
# import data.daily_data
from dateutil.relativedelta import relativedelta


def get_last_date_of_month(year: int, month: int) -> date:
    """Given a year and a month returns an instance of the date class
    containing the last day of the corresponding month.
    Source: https://stackoverflow.com/questions/42950/get-last-day-of-the-month-in-python
    """
    return date(year, month, monthrange(year, month)[1])


def convert_dates_to_timeframe(start: date, stop: date) -> str:
    """Given two dates, returns a stringified version of the interval between
    the two dates which is used to retrieve Data for a specific time frame
    from Google Trends.
    """
    return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"


def GetNewProxy(pytrends):
    """
    Increment proxy INDEX; zero on overflow
    """
    if pytrends.proxy_index < (len(pytrends.proxies) - 1):
        pytrends.proxy_index += 1
    else:
        pytrends.proxy_index = 0


def _fetch_data(pytrends, build_payload, timeframe: str) -> pd.DataFrame:
    """Attempts to fecth Data and retries in case of a ResponseError."""
    attempts, fetched, res = 0, False, False
    while not fetched:
        try:
            # r1 = random.randint(0, 120)
            # sleep(r1)
            build_payload(timeframe=timeframe)
            # if ResponseError == 429:
            #     print("its come")
            #     time.sleep(int(pytrends.headers["Retry-After"]))
        except Exception:
            # print("Connection refused")
            # print(err)
            print(f' The error in build_payload,trying again in {60 + 5 * attempts} seconds.')
            sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
        else:
            fetched = True
    try:
        return pytrends.interest_over_time()
    except:
        while not res:
            sleep(60 + 5 * attempts)
            attempts += 1
            print(f"The error in interest_over_time , trying again in {60 + 5 * attempts} seconds")
            try:
                return pytrends.interest_over_time()
            except:
                attempts += 1
            else:
                res = True


def get_daily_data(word: str, keyword_file: str,
                   start_year: int,
                   start_mon: int,
                   stop_year: int,
                   stop_mon: int,
                   geo: str = 'US',
                   verbose: bool = True,
                   wait_time: float = 120) -> pd.DataFrame:
    """Given a word, fetches daily search volume Data from Google Trends and
    returns results in a pandas DataFrame.
    Details: Due to the way Google Trends scales and returns Data, special
    care needs to be taken to make the daily Data comparable over different
    months. To do that, we download daily Data on a month by month basis,
    and also monthly Data. The monthly Data is downloaded in one go, so that
    the monthly values are comparable amongst themselves and can be used to
    scale the daily Data. The daily Data is scaled by multiplying the daily
    value by the monthly search volume divided by 100.
    For a more detailed explanation see http://bit.ly/trendsscaling
    Args:
        word (str): Word to fetch daily Data for.
        start_year (int): the start year
        start_mon (int): start 1st day of the month
        stop_year (int): the end year
        stop_mon (int): end at the last day of the month
        geo (str): geolocation
        verbose (bool): If True, then prints the word and current time frame
            we are fecthing the Data for.
    Returns:
        complete (pd.DataFrame): Contains 4 columns.
            The column named after the word argument contains the daily search
            volume already scaled and comparable through time.
            The column f'{word}_unscaled' is the original daily Data fetched
            month by month, and it is not comparable across different months
            (but is comparable within a month).
            The column f'{word}_monthly' contains the original monthly Data
            fetched at once. The values in this column have been backfilled
            so that there are no NaN present.
            The column 'scale' contains the scale used to obtain the scaled
            daily Data.
    """

    # Set up start and stop dates
    # start_date = date(start_year, start_mon, 1)
    # stop_date = get_last_date_of_month(stop_year, stop_mon)
    #
    # # Start pytrends for US region
    # pytrends = TrendReq(hl='en-US', tz=360)
    # # Initialize build_payload with the word we need Data for
    # build_payload = partial(pytrends.build_payload,
    #                         kw_list=[word], cat=0, geo=geo, gprop='')
    #
    # # Obtain monthly Data for all months in years [start_year, stop_year]
    # monthly = _fetch_data(pytrends, build_payload,
    #                       convert_dates_to_timeframe(start_date, stop_date))
    #
    # # Get daily Data, month by month
    # results = {}
    # # if a timeout or too many requests error occur we need to adjust wait time
    # current = start_date
    # while current < stop_date:
    #     last_date_of_month = get_last_date_of_month(current.year, current.month)
    #     timeframe = convert_dates_to_timeframe(current, last_date_of_month)
    #     if verbose:
    #         print(f'{word}:{timeframe}')
    #     results[current] = _fetch_data(pytrends, build_payload, timeframe)
    #     current = last_date_of_month + relativedelta(days=+7)
    #     sleep(randrange(10, 10 * wait_time) / 10)  # don't go too fast or Google will send 429s
    #
    # daily = pd.concat(results.values()).drop(columns=['isPartial'])
    # complete = daily.join(monthly, lsuffix='_unscaled', rsuffix='_monthly')
    #
    # # Scale daily Data by monthly weights so the Data is comparable
    # complete[f'{word}_monthly'].ffill(inplace=True)  # fill NaN values
    # complete['scale'] = complete[f'{word}_monthly'] / 100
    # complete[word] = complete[f'{word}_unscaled'] * complete.scale
    # complete = pd.DataFrame(complete)
    # # complete = complete.iloc[::, 4:5]
    # print(complete)
    # complete.to_csv(rf'C:\Users\Ayala~\Documents\אילה\שנה ב\project\data\daily_data\{keyword_file}.csv',
    #                 index=True)

def get_last_date_in_year(date_1):
    return date(date_1.year, 12, 31)

def get_data_of_week(word,num, date_min, date_max):
    # days_in_week = pd.DataFrame()
    # d = date_min
    s = pd.date_range(date_min, date_max, freq='D')
    s = pd.DataFrame(s,columns=['date'])
    s[word] = num
    return s
    # while d < date_max:
    #     pd.concat([days_in_week, pd.where(data["data"] == date_min)])
    #     d = d + relativedelta(days=+1)
    # print(days_in_week)
    # return days_in_week


def get_data(word: str, keyword_file: str,
          start_year: int,
          start_mon: int,
          stop_year: int,
          stop_mon: int,
          geo: str = 'US',
          verbose: bool = True,
          wait_time: float = 120) -> pd.DataFrame:
    # Set up start and stop dates
    start_date = date(start_year, start_mon, 1)
    stop_date = get_last_date_of_month(stop_year, stop_mon)

    pytrends = TrendReq(hl='en-US', tz=360)
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word], cat=0, geo=geo, gprop='')

    current = start_date
    weekly = pd.DataFrame()
    daily = pd.DataFrame()
    complete = pd.DataFrame()

    while current < stop_date:
        stop_midlle_year_date = get_last_date_in_year(current)
        # stop_midlle_year_date = current + relativedelta(years=+5)
        # if(stop_midlle_year_date>stop_date):
        #     stop_midlle_year_date=stop_date

        weekly_year = _fetch_data(pytrends, build_payload,
                                  convert_dates_to_timeframe(current, stop_midlle_year_date))
        print(word,current)

        df_temp_weekly = pd.DataFrame(weekly_year[word].values, columns=[word])
        df_temp_weekly["date"] = weekly_year.index
        weekly= pd.concat([weekly,df_temp_weekly],axis=0,ignore_index=True)
        sleep(randrange(10, 10 * wait_time) / 10)
        current_month = current

        while current_month < stop_midlle_year_date:
            stop_middle_month = get_last_date_of_month(current_month.year, current_month.month)
            # stop_middle_month = current_month + relativedelta(months=+6)
            daily_month = _fetch_data(pytrends, build_payload,
                                      convert_dates_to_timeframe(current_month, stop_middle_month))
            print(" ",current_month)
            df_temp_daily = pd.DataFrame(daily_month[word].values, columns=[word])
            df_temp_daily["date"] = daily_month.index
            daily = pd.concat([daily, df_temp_daily],axis=0,ignore_index=True)
            # daily= daily.append(df_temp_daily)
            # daily.join(df_temp_daily)
            current_month = current_month + relativedelta(months=+1)
            # sleep(randrange(10, 10 * wait_time) / 10)

        current = current + relativedelta(years=+1)
        df_all_weekly_on_daily = pd.DataFrame(columns=['date',word])
        for i in range(weekly.shape[0]):
            date_after_week = (weekly.iloc[i].date+relativedelta(days=+6))
            temp = get_data_of_week(word,weekly.iloc[i][word], date(weekly.iloc[i].date.year,weekly.iloc[i].date.month,weekly.iloc[i].date.day),date(date_after_week.year,date_after_week.month,date_after_week.day) )
            df_all_weekly_on_daily = pd.concat([df_all_weekly_on_daily, temp],axis=0,ignore_index=True)


    daily.to_csv(rf'C:\Users\Ayala~\Documents\אילה\שנה ב\project\data\daily_data\{keyword_file}.csv',
                    index=True)
    df_all_weekly_on_daily.to_csv(rf'C:\Users\Ayala~\Documents\אילה\שנה ב\project\data\weekly_data\{keyword_file}.csv',
                    index=True)

    print("daily_data")
    print(daily)
    print("weekle_data")
    print(weekly)

    result = {}
    # Start pytrends for US region

    # stop_date = get_last_date_of_month(start_date.year,start_date.month)
    # daily_monthly = _fetch_data(pytrends, build_payload,
    #                       convert_dates_to_timeframe(start_date, stop_date))
    # stop_date = start_date  + relativedelta(years=+1)
    # weekly_year = _fetch_data(pytrends, build_payload,
    #                       convert_dates_to_timeframe(start_date, stop_date))
    # stop_date = stop_date + relativedelta(years=+10)
    # monthly_10years = _fetch_data(pytrends, build_payload,
    #                       convert_dates_to_timeframe(start_date, stop_date))
    #
    # # print(daily_monthly,"daily")
    # # print(daily_monthly.columns,"daily")
    #
    # df1=pd.DataFrame(daily_monthly['debt'].values,columns=['debt'])
    # df1["date"]=daily_monthly.index
    #
    # df2 = pd.DataFrame(weekly_year['debt'].values, columns=['debt'])
    # df2["date"] = weekly_year.index
    # df3 = pd.DataFrame(monthly_10years['debt'].values, columns=['debt'])
    # df3["date"] = monthly_10years.index
    # print(df1.shape,df2.shape,df3.shape)
    # values_weekly_year=weekly_year[]
    # df['weekly']=

    # df=pd.DataFrame(daily_monthly,columns=['date','Daily'])
    # df['weekly'] = weekly_year
    # df['monthly'] = monthly_10years
    # print("data",df)
    # print(weekly_year,"weekly")
    # print(monthly_10years,"monthly")
    # index_week=weekly_year.index[0:4]

    # print(df2)
    # print(daily_monthly.index[3],weekly_year.index[0],weekly_year.index[1])
    # print(get_date(df2,daily_monthly.index[3],index_week[0],index_week[1]))
    # # df1['weekly_year']=weekly_year[]
    # print(index_week)

    # data = [["daily_monthly", daily_monthly], ["weekly_year", weekly_year], ["monthly_10years", monthly_10years]]
    #
    # df = pd.DataFrame(data)
    # print(df)
    # df.to_csv(rf'C:\Users\Ayala~\Documents\אילה\שנה ב\project\data\daily_data\{"nisuy"}.csv',
    #                 # index=True)


# keyword_file = "debt_nisuy"
# nisuy("color", keyword_file, 2021, 1, 2021 ,12)
with open("keywords.txt") as f:
    end_date = date.today()
    for line in f:
        keyword = line.strip()
        keyword_file = keyword.replace(' ', '_')
        get_data(keyword, keyword_file, 2004, 1, 2022, 12)


# def get_data_of_week(data,date,date_min,date_max):
#     if date<date_max and date>date_min:
#         print(date_min)
#         print(data.index[0])
#         return data[date==date_min]
#     else:
#         return None
