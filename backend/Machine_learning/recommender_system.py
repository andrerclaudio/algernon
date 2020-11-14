# Build-in modules
import logging
from queue import Queue
from threading import Thread, ThreadError

# Added modules
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Project modules
from Book.client import good_reads_client as good_reads
from messages import send_message_object as send

logger = logging.getLogger(__name__)


class CategoricalNumericalLib(object):
    """Create a dictionary with labels and its respective number format"""

    def __init__(self):
        self.title = {}
        self.author = {}
        self.publisher = {}
        self.shelf = {}

    def convert_label(self, data, title=None, author=None, publisher=None, shelf=None):

        previous_dict = []

        if title:
            previous_dict = self.title
        elif author:
            previous_dict = self.author
        elif publisher:
            previous_dict = self.publisher
        elif shelf:
            previous_dict = self.shelf

        if len(data) > 0:
            x = len(previous_dict) + 1
            for element in data:
                if element not in previous_dict.keys():
                    previous_dict[element] = x
                    x += 1

            if title:
                self.title = previous_dict
            elif author:
                self.author = previous_dict
            elif publisher:
                self.publisher = previous_dict
            elif shelf:
                self.shelf = previous_dict


def recommendation_tree():
    """A book recommendation system based in books you read in the past"""
    # Number of books to take in count making prediction
    n_books = 8
    # Create a Dataframe with some book information
    column_names = ['GID', 'ISBN', 'TITLE', 'AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
    qty_popular_shelf = 100
    others = [str(i) for i in range(qty_popular_shelf)]
    column_names.extend(others)
    # Initialize the categorical-numerical library
    cat_num = CategoricalNumericalLib()
    # Books to be parsed
    books = []
    # Flag about Prediction ability
    prediction_problem = True
    # Hold read books Gid
    books_gid = []




    if len(books) > 0:
        # Build the prediction database and fetch its similar books
        df_predict, similar_books = set_dataframe(books, column_names, cat_num)

        if not df_predict.empty:

            books_gid.extend((df_predict['GID']).tolist())

            # Check if there are similar books
            if len(similar_books) > 0:
                # Build the training database
                training_database, similar_books = set_dataframe(similar_books, column_names, cat_num,
                                                                 isbn_values=False)

                if not training_database.empty:

                    # Adjust the dataframe information
                    col_names = column_names[7:]
                    col = ['AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
                    col.extend(col_names)

                    y_pred = run_prediction(training_database, df_predict, col)

                    if len(y_pred) > 0:

                        for gid in y_pred:
                            training_database.drop(training_database[training_database['GID'] == gid].index,
                                                   inplace=True)

                        training_database.reset_index()

                        new_isbn = []
                        for gid in y_pred:
                            info = book_information_lookup(book_id=gid)
                            if info is not False:
                                new_isbn.append(int(info.isbn13) if info.isbn13 is not None else int(0))

                        if len(new_isbn) > 0:
                            df_predict, similar_books = set_dataframe(new_isbn, column_names, cat_num)

                            if not df_predict.empty:
                                y = run_prediction(training_database, df_predict, col)
                                y_pred.extend(y)

                                # Remove redundancy items
                                y_pred = list(set(y_pred))

                        for gid in y_pred:
                            if str(gid) not in str(books_gid):
                                info = book_information_lookup(book_id=str(gid))
                                if info is not False:
                                    # Indicates that we were able to predicts the next book
                                    prediction_problem = False

    logger.info('Recommendation task finished!')

    return


def run_prediction(train_database, predict_database, columns):
    """ """
    x_train = train_database[columns]
    y_train = train_database['GID']

    # Create a Random Forest Classifier
    logger.info('Running Random Forest Classifier!')
    clf = RandomForestClassifier(n_estimators=100)

    # logger.info('Running Neural Network!')
    # clf = MLPClassifier(hidden_layer_sizes=(100,), random_state=1, max_iter=300, solver='adam', activation='tanh')

    # Create a SVM Classifier
    # logger.info('Running SVM Classifier!')
    # clf = SVC(kernel='rbf')

    # Train the model
    clf.fit(x_train, y_train)

    x_test = predict_database[columns]
    y_pred = clf.predict(x_test)

    return list(set(y_pred))


def sorting_rows(dataframe):
    """A simple row sorting scheme"""
    sorted_values = []
    rows = dataframe.values
    for row in rows:
        row_tail = sorted((row[7:]).tolist())
        row_header = (row[0:7]).tolist()
        row_header.extend(row_tail)
        sorted_values.append(row_header)

    return sorted_values


def database_lookup(books, max_row_qty):
    """ """
    number_of_desire_thread = 32
    new_threads = 0
    activities_count = len(books)
    count = 0

    # Initialize data sharing
    info_queue = Queue()
    book_queue = Queue()
    running_processes = []

    logger.info('Starting fetching process ...')

    while len(books) > 0:
        running_processes.clear()
        while (new_threads <= number_of_desire_thread - 1) and (len(books) > 0):
            name = books.pop()
            try:
                t = Thread(target=book_info_organizer, args=[name, info_queue, book_queue, max_row_qty])

                t.daemon = True  # Daemonize thread
                t.start()  # Start the execution
                running_processes.append(t)
                new_threads += 1
            except ThreadError as e:
                logger.exception('{}'.format(e), exc_info=False)
                books.append(name)
        # Wail for all processes to finish
        [t.join() for t in running_processes]
        count += new_threads
        logger.info('Fetched {} of {}'.format(count, activities_count))
        new_threads = 0

    book_info = []
    while not info_queue.empty():
        book_info.append(info_queue.get())

    similar_books = []
    try:
        while not book_queue.empty():
            [similar_books.append(str(i)) for i in book_queue.get()]
    except Exception as e:
        logger.exception(e, exc_info=False)
    finally:
        # Take unique values
        similar_books = list(set(similar_books))

    return book_info, similar_books


def set_dataframe(books, column_names, cat_num, isbn_values=True):
    """ """
    book_name = []

    # Predict database
    database = pd.DataFrame(columns=column_names)

    if isbn_values:
        for isbn in books:
            info = book_information_lookup(isbn=isbn)
            if info is not False:
                book_name.append(str(info.title) if info.title is not None else str('_'))
    else:
        book_name = books

    if len(book_name) > 0:

        # For a given book list, find its information and similar books
        book_info, similar_books = database_lookup(book_name, len(column_names))

        # Add all info into a Pandas Dataframe
        for data in book_info:
            database.loc[len(database)] = data

        # Check if we have material to make predictions
        if not database.empty:
            cat_num.convert_label((database['TITLE'].tolist()), title=True)
            database.replace({'TITLE': cat_num.title}, inplace=True)

            cat_num.convert_label((database['AUTHOR'].tolist()), author=True)
            database.replace({'AUTHOR': cat_num.author}, inplace=True)

            cat_num.convert_label((database['PUBLISHER'].tolist()), publisher=True)
            database.replace({'PUBLISHER': cat_num.publisher}, inplace=True)

            values = []
            col = [i for i in database.columns][7:]
            [values.extend(database[i].tolist()) for i in col]

            cat_num.convert_label(values, shelf=True)
            [database.replace({i: cat_num.shelf}, inplace=True) for i in col]

            sorted_values = sorting_rows(database)

            df = pd.DataFrame(sorted_values, columns=column_names)

            # Take unique values
            similar_books = list(set(similar_books))

            return df, similar_books

    return database, book_name


def book_info_organizer(book_name, book_info, similar_books, max_row_qty):
    """ """
    list_info = []
    books = []

    # Find Good Reads Id
    gid = find_good_reads_id(str(book_name))
    if int(gid) > 0:
        # Fetch book information
        info = book_information_lookup(book_id=gid)
        if info is not False:
            b_isbn = int(info.isbn13) if info.isbn13 is not None else int(0)
            b_gid = int(info.gid) if info.gid is not None else int(0)
            b_title = str(info.title) if info.title is not None else str('_')
            b_average_rating = float(info.average_rating) if info.average_rating is not None else int(0)
            b_ratings_count = int(info.ratings_count) if info.ratings_count is not None else int(0)
            b_author = str(info.authors[0]) if info.authors[0] is not None else str('_')
            b_publisher = str(info.publisher) if info.publisher is not None else str('_')

            list_info.extend([b_gid, b_isbn, b_title, b_average_rating, b_ratings_count, b_author, b_publisher])

            for j in info.popular_shelves:
                if len(list_info) == max_row_qty:
                    break
                list_info.append(str(j))

            if len(list_info) < max_row_qty:
                while len(list_info) < max_row_qty:
                    list_info.append(None)

            try:
                if len(info.similar_books) > 0:
                    ret = list(info.similar_books)
                    # _ = ret.pop()
                    books.extend(ret)
                    similar_books.put(books)
            except Exception as e:
                logger.exception(e, exc_info=False)

            if len(list_info) > 0:
                book_info.put(list_info)


def book_information_lookup(book_id=None, isbn=None):
    """Get info about a book"""
    info = False
    try:
        if book_id:
            info = good_reads.book(book_id=book_id)
        elif isbn:
            info = good_reads.book(isbn=isbn)
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
    finally:
        return info


def find_good_reads_id(name):
    """Return the good reads Id given a Book Name"""
    gid = 0
    try:
        info = good_reads.search_books(str(name))
        if str(name).lower() in str(info).lower():
            x = 0
            for i in info:
                if str(name).lower() != str(i).lower():
                    x += 1
                else:
                    break
            if x < len(info):
                gid = info[x].gid
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
    finally:
        return gid
