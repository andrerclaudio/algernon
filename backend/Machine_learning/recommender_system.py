# Build-in modules
import logging
from queue import Queue
from threading import Thread, ThreadError

# Added modules
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Project modules
from Book.client import good_reads_client as good_reads

logger = logging.getLogger(__name__)

NUMBER_MAX_THREADS = 32

# Create a Dataframe book information
COLUMNS_NAMES = ['GID', 'ISBN', 'TITLE', 'AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
qty_popular_shelf = 100
others_columns = [str(i) for i in range(qty_popular_shelf)]
COLUMNS_NAMES.extend(others_columns)
QTY_OF_ROWS = len(COLUMNS_NAMES)


class CategoricalToNumericalConverter(object):
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

    # Initialize the categorical-numerical library
    converter = CategoricalToNumericalConverter()

    similar_books = []
    # Books to be parsed
    isbn_list = ['9788532530783', '9788556510785']

    if len(isbn_list) > 0:

        # Fetch all related books ISBN codes information
        information = set_information(isbn_list)

        for items in information:
            if len(items.similar_books) > 0:
                for similar in items.similar_books:
                    similar_books.append(similar)

        books_prediction_df = pd.DataFrame(columns=COLUMNS_NAMES)
        list_info = []

        for info in information:

            temp = []
            b_isbn = int(info.isbn13) if info.isbn13 is not None else int(0)
            b_gid = int(info.gid) if info.gid is not None else int(0)
            b_title = str(info.title) if info.title is not None else str('_')
            b_average_rating = float(info.average_rating) if info.average_rating is not None else int(0)
            b_ratings_count = int(info.ratings_count) if info.ratings_count is not None else int(0)
            b_author = str(info.authors[0]) if info.authors[0] is not None else str('_')
            b_publisher = str(info.publisher) if info.publisher is not None else str('_')

            temp.extend([b_gid, b_isbn, b_title, b_average_rating, b_ratings_count, b_author, b_publisher])

            for j in info.popular_shelves:
                if len(temp) == QTY_OF_ROWS:
                    break
                temp.append(str(j))

            if len(temp) < QTY_OF_ROWS:
                while len(temp) < QTY_OF_ROWS:
                    temp.append(None)

            if len(temp) > 0:
                list_info.append(temp)

        # Add all info into a Pandas Dataframe
        for data in book_info:
            books_prediction_df.loc[len(books_prediction_df)] = data




        # Build the prediction database and fetch its similar books
        read_books_info, similar_books_info = set_dataframe(isbn_list, converter)

        if not read_books_info.empty:

            # Check if there are similar books
            if len(similar_books_info) > 0:
                # Build the training database
                training_database, similar_books_info = set_dataframe(similar_books_info, converter)

                if not training_database.empty:

                    # Adjust the dataframe information
                    col_names = COLUMNS_NAMES[7:]
                    col = ['AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
                    col.extend(col_names)

                    y_pred = run_prediction(training_database, read_books_info, col)

                    if len(y_pred) > 0:

                        for gid in y_pred:
                            training_database.drop(training_database[training_database['GID'] == gid].index,
                                                   inplace=True)

                        training_database.reset_index()

                        new_isbn = []
                        for gid in y_pred:
                            info = book_information_lookup(gid=gid)
                            if info is not False:
                                new_isbn.append(int(info.isbn13) if info.isbn13 is not None else int(0))

                        if len(new_isbn) > 0:
                            read_books_info, similar_books_info = set_dataframe(new_isbn, converter)

                            if not read_books_info.empty:
                                y = run_prediction(training_database, read_books_info, col)
                                y_pred.extend(y)

                                # Remove redundancy items
                                y_pred = list(set(y_pred))

                        for gid in y_pred:
                            if str(gid) not in str(books_gid):
                                info = book_information_lookup(gid=str(gid))
                                if info is not False:
                                    # Indicates that we were able to predicts the next book
                                    prediction_problem = False

    logger.info('Recommendation task finished!')

    return


def set_information(isbn_list):
    """ """
    new_threads = 0
    count = 0
    info = []
    running_processes = []

    # Initialize data sharing
    info_queue = Queue()

    logger.info('Starting fetching process ...')

    while len(isbn_list) > 0:
        running_processes.clear()
        while (new_threads <= NUMBER_MAX_THREADS - 1) and (len(isbn_list) > 0):
            isbn_code = isbn_list.pop()
            try:
                t = Thread(target=book_information_lookup, args=[isbn_code, info_queue])

                t.daemon = True  # Daemonize thread
                t.start()  # Start the execution
                running_processes.append(t)
                new_threads += 1
            except ThreadError as e:
                logger.exception('{}'.format(e), exc_info=False)
                isbn_list.append(isbn_code)
        # Wail for all processes to finish
        [t.join() for t in running_processes]
        count += new_threads
        logger.info('Remaining {} ISBNs codes to fetch.'.format(len(isbn_list)))
        new_threads = 0

    while not info_queue.empty():
        ret = info_queue.get()
        if ret is not False:
            info.append(ret)

    return info


def book_information_lookup(isbn, info_queue):
    """Get info about a book"""
    info = False
    try:
        info = good_reads.book(isbn=isbn)
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
    finally:
        info_queue.put(info)















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





def set_dataframe(isbn_list, converter):
    """ """
    book_name = []

    # Predict database
    database = pd.DataFrame(columns=COLUMNS_NAMES)

    for isbn in isbn_list:
        info = book_information_lookup(isbn=isbn)
        if info is not False:
            book_name.append(str(info.title) if info.title is not None else str('_'))

    if len(book_name) > 0:

        # For a given book list, find its information and similar books
        book_info, similar_books = set_information(isbn_list)

        # Add all info into a Pandas Dataframe
        for data in book_info:
            database.loc[len(database)] = data

        # Check if we have material to make predictions
        if not database.empty:
            converter.convert_label((database['TITLE'].tolist()), title=True)
            database.replace({'TITLE': converter.title}, inplace=True)

            converter.convert_label((database['AUTHOR'].tolist()), author=True)
            database.replace({'AUTHOR': converter.author}, inplace=True)

            converter.convert_label((database['PUBLISHER'].tolist()), publisher=True)
            database.replace({'PUBLISHER': converter.publisher}, inplace=True)

            values = []
            col = [i for i in database.columns][7:]
            [values.extend(database[i].tolist()) for i in col]

            converter.convert_label(values, shelf=True)
            [database.replace({i: converter.shelf}, inplace=True) for i in col]

            sorted_values = sorting_rows(database)

            df = pd.DataFrame(sorted_values, columns=COLUMNS_NAMES)

            # Take unique values
            similar_books = list(set(similar_books))

            return df, similar_books

    return database, book_name


def book_info_organizer(isbn, book, similar):
    """ """
    list_info = []
    books = []

    # Fetch book information
    book_information_lookup(isbn=isbn)

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
            if len(list_info) == QTY_OF_ROWS:
                break
            list_info.append(str(j))

        if len(list_info) < QTY_OF_ROWS:
            while len(list_info) < QTY_OF_ROWS:
                list_info.append(None)

        try:
            if len(info.similar_books) > 0:
                ret = list(info.similar_books)
                books.extend(ret)
                similar.put(books)
        except Exception as e:
            logger.exception(e, exc_info=False)

        if len(list_info) > 0:
            book.put(list_info)



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