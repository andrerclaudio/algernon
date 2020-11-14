# Build-in modules
import logging
import time
from datetime import datetime, timedelta
from math import sqrt

# Added modules
import matplotlib
import numpy as np
import pytz
from sklearn import exceptions
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

matplotlib.use('Agg')
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


def finish_prediction_polynomial(tables, chat_id):
    """
    Predicts when the current book will finish using polynomial regression
    """
    delta = polynomial_regression(tables, chat_id)
    return calculate_date(tables, delta)


def clustering(tables):
    """
    Group similar reading periods and return these ones as a time format.
    """
    timestamp = k_means_clustering(tables)
    if timestamp is not False:
        timestamp = timestamp.tolist()
        active = [datetime.fromtimestamp(int(values), tz=pytz.utc) for values in timestamp]
        return active
    else:
        return False


def calculate_date(tables, delta):
    """Return a string with a date based on a delta"""
    if delta > 0:

        df = tables.get_value('tREADING')
        # Column selector
        start = df['START_DAY'][df.index[0]]

        new = datetime.fromtimestamp(start) + timedelta(days=delta)
        date_projection = int(new.timestamp())
        local_time_adjust = time.localtime(date_projection)

        return time.strftime('%A, %d %b %Y', local_time_adjust)
    else:
        return False


def week_days_reading(tables, chat_id):
    """It shows which days in the week the user reads most"""

    week_days = {'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0,
                 'Friday': 0, 'Saturday': 0, 'Sunday': 0, }

    df = tables.get_value('tPAGES')
    if df is not None:
        days_datetime = [datetime.fromtimestamp(days) for days in df['TIMESTAMP'].tolist()]
        qty = df['QUANTITY'].tolist()
        total = sum(qty)

        if total > 0:
            for days in days_datetime:
                week_day = days.weekday()
                idx = days_datetime.index(days)
                value = qty[idx]

                if week_day == 0:
                    week_days['Monday'] = week_days['Monday'] + value
                elif week_day == 1:
                    week_days['Tuesday'] = week_days['Tuesday'] + value
                elif week_day == 2:
                    week_days['Wednesday'] = week_days['Wednesday'] + value
                elif week_day == 3:
                    week_days['Thursday'] = week_days['Thursday'] + value
                elif week_day == 4:
                    week_days['Friday'] = week_days['Friday'] + value
                elif week_day == 5:
                    week_days['Saturday'] = week_days['Saturday'] + value
                else:
                    week_days['Sunday'] = week_days['Sunday'] + value

            max_value = max(week_days, key=week_days.get)
            min_value = min(week_days, key=week_days.get)

            labels = list(week_days.keys())
            sizes = [(val / total) for val in list(week_days.values())]

            plt.clf()
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
            ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
            plt.savefig('./Database/pie_{}.png'.format(chat_id))

            return max_value, min_value
        else:
            return False, False
    else:
        return False, False


def global_mean(database):
    """
    Global mean of pages read.
    """
    df = database.get_value('tPAGES')
    if df is None:
        return int(0)
    else:
        return df['QUANTITY'].mean(skipna=True)


def polynomial_regression(tables, chat_id):
    """Return the delta to finished the current reading"""
    max_func_degree = 3
    min_func_degree = 1

    global_average = global_mean(tables)
    if global_average > 0:
        try:
            df = tables.get_value('tREADING')

            # Column selector
            pages = df['BOOK_PAGE']
            end = df['END_PAGE'][df.index[0]]

            # Shape adjustments
            y_train = (pages.to_numpy()).reshape(-1, 1)

            # Indices creator
            x = [n for n in range(0, len(y_train))]
            x_train = np.array(x).reshape(-1, 1)

            target_page = False
            target_degree = max_func_degree
            x_index = []
            y_pred = []

            while (target_degree >= min_func_degree) and (target_page is False):
                # Initializing variable before predicting values
                x_index.clear()
                y_pred.clear()
                index = 0
                prediction = 0
                previous_prediction = -1

                # Fitting Polynomial Regression to the dataset
                poly_reg = PolynomialFeatures(degree=target_degree)
                x_poly = poly_reg.fit_transform(x_train)
                pol_reg = LinearRegression(fit_intercept=True, normalize=True)
                pol_reg.fit(x_poly, y_train)

                while previous_prediction < prediction < end:
                    # Save the last predicted value
                    previous_prediction = prediction

                    # Predicting a new result with Polynomial Regression
                    prediction = pol_reg.predict(poly_reg.fit_transform([[index]]))
                    y_pred.append(prediction[0, 0])
                    x_index.append(index)
                    index += 1

                if prediction >= end:
                    target_page = True
                else:
                    target_degree -= 1

            if target_page:

                delta = len(x_index) - 1

                plt.clf()
                plt.scatter(x_train, y_train, color='red')
                plt.scatter(x_index[-1], y_pred[-1], marker='x', color='orange')
                plt.plot(x_index, y_pred, color='blue')
                plt.title('Finish prediction')
                plt.ylabel('Total pages')
                plt.xlabel('Delta')
                plt.grid()
                plt.savefig('./Database/polynomial_regression_{}.png'.format(chat_id))

                return delta
            else:
                return 0
        except exceptions as e:
            logger.exception('{}'.format(e))
            return 0
    else:
        return 0


def k_means_clustering(database):
    """
    Return all principal hours the user is reading
    """
    qty_clusters_min = 1
    qty_clusters_max = 4

    df = database.get_value('tPAGES')
    if df is not None:
        # Column selector
        timestamp = (df['TIMESTAMP']).to_list()
        pages = (df['QUANTITY']).to_list()

        new = []
        idx = 0
        for value in pages:
            if value != 0:
                new.append(timestamp[idx])
            idx += 1

        timestamp = new

        if len(timestamp) > 1:
            try:
                # Extract seconds from timestamp
                values = [datetime.fromtimestamp(i) for i in timestamp]
                seconds = [(val.hour * 3600) + (val.minute * 60) + val.second for val in values]
                # Shape adjustments
                y_train = (np.array(seconds)).reshape(-1, 1)
                # calculating the within clusters sum-of-squares for 19 cluster amounts
                sum_of_squares = calculate_wcss(y_train, qty_clusters_min, qty_clusters_max)

                if sum_of_squares is not False:
                    # calculating the optimal number of clusters
                    n_centers = optimal_number_of_clusters(sum_of_squares, qty_clusters_min, qty_clusters_max)
                    # Check if there is a convergence
                    if n_centers > 0:
                        km = KMeans(n_clusters=n_centers)
                        y_km = km.fit_predict(y_train)
                        centers = km.cluster_centers_
                        return centers[:, 0]
                    else:
                        return False
                else:
                    return False
            except exceptions as e:
                logger.exception('{}'.format(e))
                return False
        else:
            return False
    else:
        return False


def calculate_wcss(data, cluster_min, cluster_max):
    """

    """
    wcss = []
    try:
        for n in range(cluster_min, cluster_max):
            km = KMeans(n_clusters=n)
            km.fit(X=data)
            wcss.append(km.inertia_)
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
        return False

    return wcss


def optimal_number_of_clusters(wcss, cluster_min, cluster_max):
    """

    """
    x1, y1 = cluster_min, wcss[0]
    x2, y2 = cluster_max, wcss[len(wcss) - 1]

    distances = []
    for i in range(len(wcss)):
        x0 = i + 2
        y0 = wcss[i]
        numerator = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1)
        denominator = sqrt((y2 - y1) ** 2 + (x2 - x1) ** 2)
        distances.append(numerator / denominator)

    return distances.index(max(distances)) + 2


def historical_prediction(database, chat_id):
    """ """
    pass
