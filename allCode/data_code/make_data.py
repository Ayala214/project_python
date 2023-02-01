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


class Data():
    def __init__(self):

        """
        Args:

        """
        with open("keywords.txt") as f:
            for line in f:
                self.keyword = line.strip()
                self.keyword_file = self.keyword.replace(' ', '_')
                self.get_data(self.keyword, self.keyword_file, 2004, 1, 2011, 12)
                self.duplicate_days_of_the_week()
                self.adjust_weekly()
                self.adjust_daily()

                # Make sure that the keyword hasn't already been downloaded.
                # if os.path.exists(f'data/raw/weekly/{self.keyword_file}.csv') and os.path.exists(f'data/raw/daily/{self.keyword_file}.csv'):
                #     continue



            self.download()



    def get_last_date_of_month(self,year: int, month: int) -> date:
        """Given a year and a month returns an instance of the date class
        containing the last day of the corresponding month.
        Source: https://stackoverflow.com/questions/42950/get-last-day-of-the-month-in-python
        """
        return date(year, month, monthrange(year, month)[1])


    def convert_dates_to_timeframe(self,start: date, stop: date) -> str:
        """Given two dates, returns a stringified version of the interval between
        the two dates which is used to retrieve Data for a specific time frame
        from Google Trends.
        """
        return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"


    # def GetNewProxy(pytrends):
    #     """
    #     Increment proxy INDEX; zero on overflow
    #     """
    #     if pytrends.proxy_index < (len(pytrends.proxies) - 1):
    #         pytrends.proxy_index += 1
    #     else:
    #         pytrends.proxy_index = 0

    def _fetch_data(self,pytrends, build_payload, timeframe: str) -> pd.DataFrame:
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



    def get_data(self,word: str, keyword_file: str,
                 start_year: int,
                 start_mon: int,
                 stop_year: int,
                 stop_mon: int,
                 geo: str = 'US',
                 verbose: bool = True,
                 wait_time: float = 5) -> pd.DataFrame:

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
        start_date = date(start_year, start_mon, 1)
        stop_date = self.get_last_date_of_month(stop_year, stop_mon)

        pytrends = TrendReq(hl='en-US', tz=360)
        build_payload = partial(pytrends.build_payload,
                                kw_list=[word], cat=0, geo=geo, gprop='')
        self.monthly = self._fetch_data(pytrends, build_payload,
                                      self.convert_dates_to_timeframe(start_date, stop_date))

        current = start_date
        self.weekly = pd.DataFrame()
        self.daily = pd.DataFrame()

        while current < stop_date:
            stop_midlle_year_date = self.get_last_date_in_year(current)
            weekly_year = self._fetch_data(pytrends, build_payload,
                                      self.convert_dates_to_timeframe(current, stop_midlle_year_date))
            print(word, current)
            df_temp_weekly = pd.DataFrame(weekly_year[word].values, columns=[word])
            df_temp_weekly["date"] = weekly_year.index
            self.weekly = pd.concat([self.weekly, df_temp_weekly], axis=0,ignore_index=True)
            sleep(randrange(10, 10 * wait_time) / 10)
            current_month = current

            while current_month < stop_midlle_year_date:
                stop_middle_month = self.get_last_date_of_month(current_month.year, current_month.month)
                daily_month = self._fetch_data(pytrends, build_payload,
                                          self.convert_dates_to_timeframe(current_month, stop_middle_month))
                print(" ", current_month)
                df_temp_daily = pd.DataFrame(daily_month[word].values, columns=[word])
                df_temp_daily["date"] = daily_month.index
                self.daily = pd.concat([self.daily, df_temp_daily], axis=0,ignore_index=True)
                # daily= daily.append(df_temp_daily)
                # daily.join(df_temp_daily)
                current_month = current_month + relativedelta(months=+1)
                # sleep(randrange(10, 10 * wait_time) / 10)

            current = current + relativedelta(years=+1)
        print("daily_data")
        print(self.daily)
        print("weekle_data")
        print(self.weekly)


    def get_last_date_in_year(self,date_1):
        return date(date_1.year, 12, 31)

    def duplicate_days_of_the_week(self):

        self.df_all_weekly_on_daily = pd.DataFrame(columns=['date', self.keyword])
        for i in range(self.weekly.shape[0]):
            date_after_week = (self.weekly.iloc[i].date + relativedelta(days=+6))
            temp = self.get_data_of_week(self.keyword, self.weekly.iloc[i][self.keyword],
                                         date(self.weekly.iloc[i].date.year, self.weekly.iloc[i].date.month,
                                              self.weekly.iloc[i].date.day),
                                         date(date_after_week.year, date_after_week.month, date_after_week.day))
            self.df_all_weekly_on_daily = pd.concat([self.df_all_weekly_on_daily, temp], axis=0)
        # insert the data to 1 data
        self.df_all_weekly_on_daily.to_csv(
            rf'C:\Users\tichnut\Downloads\project_python\data\data2\weekly_data\{self.keyword_file}.csv',
            index=True)
        self.daily.to_csv(rf'C:\Users\tichnut\Downloads\project_python\data\data2\daily_data\{self.keyword_file}.csv',
                          index=True)



    def get_data_of_week(self,word,num, date_min, date_max):
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

    def adjust_weekly(self):
        """
        Adjusts the weekly data, based on the monthly data.
        Puts in one data point - from the monthly data - per 5-year increment,
        and computes the data inbetween via the percentage change of the
        unadjusted weekly data.
        """

        print('Adjusting weekly data...')

        # 0's to 1's, because you can't divide by 0.
        self.monthly.replace(0, 1)
        self.weekly.replace(0, 1)

        self.weekly['percentage_change'] = \
            1 + self.weekly[self.keyword].pct_change()
        self.weekly['Adjusted'] = ''

        # Put in monthtly data point in each 5-year increment.
        finish = True
        i = 0
        while finish:
            try:
                self.weekly['Adjusted'][i * 52]= \
                    self.monthly[self.keyword][i * 12]
                i += 1
            except Exception:
                finish = False

        for i in range(len(self.weekly)):
            if self.weekly['Adjusted'][i] == '':
                prc = float(self.weekly['percentage_change'][i])
                prev_value = float(self.weekly['Adjusted'][i - 1])
                new_value = prev_value * prc
                self.weekly.loc[i, 'Adjusted'] = new_value

        self.weekly['Adjusted'] = (
                self.weekly['Adjusted'] / self.weekly['Adjusted'].max())

        self.weekly['Adjusted'] = self.weekly['Adjusted'].astype('float64')
        self.weekly['Adjusted'] = self.weekly['Adjusted'].round(2)

        self.weekly = self.weekly.drop(
            [self.keyword, 'percentage_change'], axis=1)

        self.weekly.to_csv(
            rf'C:\Users\tichnut\Downloads\project_python\data\data1\weekly_data\{self.keyword_file}.csv',
            index=True)


    def adjust_daily(self):
        """
        Adjusts the daily data, based on the weekly data.
        Puts in one data point - from the weekly data - per 6-month increment,
        and computes the data inbetween via the percentage change of the
        unadjusted daily data.
        """
        print('Adjusting daily data...')
        self.daily = self.daily.replace(0, 1)
        self.daily['percentage_change'] = 1 + \
            self.daily[self.keyword].pct_change()
        self.daily['Adjusted'] = ''

        start_increment = self.weekly['date'][0]
        end_increment = (start_increment + relativedelta(months=+1))+relativedelta(days=-1)
        end = date(2011,12,31)

        # Compute all values after the data point, within it's increment.
        i = 0
        while True:
            imported = False
            while True:
                if i >= len(self.daily):
                    break

                # Insert the weekly data points in each 6-month increment.
                if not imported:
                    try:
                        self.daily.loc[i, 'Adjusted'] = self.weekly['Adjusted'].where(
                            self.weekly['date'] == str(start_increment)).values[0]
                        imported = True
                    except ValueError:
                        pass
                else:
                    prc = float(self.daily['percentage_change'][i])
                    prev_value = float(self.daily['Adjusted'][i-1])
                    new_value = prev_value * prc
                    self.daily.loc[i, 'Adjusted'] = new_value

                start_increment += relativedelta(days=+1)
                i += 1

                if start_increment >= end_increment:
                    break

            start_increment = end_increment
            end_increment += relativedelta(months=+1)

            if start_increment > end:
                break

        # Compute all values before the data point, within it's increment.
        i = 0
        while True:
            if i >= len(self.daily) - 1:
                break

            if self.daily.loc[i, 'Adjusted'] == '':
                j = 0
                while True:
                    if self.daily.loc[i+j, 'Adjusted'] == '':
                        j += 1
                    else:
                        prc = float(self.daily['percentage_change'][i+j])
                        new_value = float(self.daily['Adjusted'][i+j])
                        prev_value = new_value / prc
                        self.daily.loc[i+j-1, 'Adjusted'] = prev_value

                        j =j- 1

                    if j <= 0:
                        break

            i += 1

        self.daily = self.daily.drop(
            [self.keyword, 'percentage_change'], axis=1)

        self.daily['Adjusted'] = (
            self.daily['Adjusted'] / self.daily['Adjusted'].max())

        self.daily['Adjusted'] = self.daily['Adjusted'].astype('float64')
        self.daily['Adjusted'] = self.daily['Adjusted'].round(2)
        # print('Adjusting daily data...')
        # self.daily = self.daily.replace(0, 1)
        # self.daily['percentage_change'] = 1 + \
        #                                   self.daily[self.keyword].pct_change()
        # self.daily['Adjusted'] = ''
        #
        # start_increment = self.daily['date'][0]
        # end_increment = start_increment + relativedelta(months=+1)
        # end = date(2022,12,31)
        #
        # # Compute all values after the data point, within it's increment.
        # i = 0
        # while True:
        #     imported = False
        #     while True:
        #         if i >= len(self.daily):
        #             break
        #
        #         # Insert the weekly data points in each 6-month increment.
        #         if not imported:
        #             try:
        #                 self.daily['Adjusted'][i] = self.weekly['Adjusted'].where(
        #                     self.weekly['date'] == str(start_increment)).values[0]
        #                 imported = True
        #             except ValueError:
        #                 break
        #         else:
        #             prc = float(self.daily['percentage_change'][i])
        #             prev_value = float(self.daily['Adjusted'][i - 1])
        #             new_value = prev_value * prc
        #             self.daily['Adjusted'][i] = new_value
        #
        #         start_increment += relativedelta(days=+1)
        #         i += 1
        #
        #         if start_increment >= end_increment:
        #             break
        #
        #     start_increment = end_increment
        #     end_increment += relativedelta(months=+1)
        #
        #     if start_increment > end:
        #         break
        #
        # # Compute all values before the data point, within it's increment.
        # i = 0
        # while True:
        #     if i >= len(self.daily) - 1:
        #         break
        #
        #     if self.daily['Adjusted'][i] == '':
        #         j = 0
        #         while True:
        #             if self.daily['Adjusted'][i + j] == '':
        #                 j += 1
        #             else:
        #                 prc = float(self.daily['percentage_change'][i + j])
        #                 new_value = float(self.daily['Adjusted'][i + j])
        #                 prev_value = new_value / prc
        #                 self.daily['Adjusted'][i + j - 1] = prev_value
        #
        #                 j -= 1
        #
        #             if j <= 0:
        #                 break
        #
        #     i += 1
        #
        # self.daily = self.daily.drop(
        #     [self.keyword, 'percentage_change'], axis=1)
        #
        # self.daily['Adjusted'] = (
        #         self.daily['Adjusted'] / self.daily['Adjusted'].max())
        #
        # self.daily['Adjusted'] = self.daily['Adjusted'].astype('float64')
        # self.daily['Adjusted'] = self.daily['Adjusted'].round(2)

        self.daily.to_csv(rf'C:\Users\tichnut\Downloads\project_python\data\data1\daily_data\{self.keyword_file}.csv',
                          index=True)



d= Data()





