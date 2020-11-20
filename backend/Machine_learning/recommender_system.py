# Build-in modules
import logging
from queue import Queue
from threading import Thread, ThreadError

# Added modules
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Project modules
from sklearn.neural_network import MLPClassifier

from Book.client import good_reads_client as good_reads

logger = logging.getLogger(__name__)

NUMBER_MAX_THREADS = 64

# Create a Dataframe book information
COLUMNS_NAMES = ['GID', 'ISBN', 'TITLE', 'AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
INITIAL_POPULAR_SHELF_INDEX = COLUMNS_NAMES.index('PUBLISHER') + 1
QTY_POPULAR_SHELF = 100
OTHERS_COLUMNS = [str(i) for i in range(QTY_POPULAR_SHELF)]
COLUMNS_NAMES.extend(OTHERS_COLUMNS)
QTY_OF_ROWS = len(COLUMNS_NAMES)


def convert_to_numerical(data, column):
    if len(data) > 0:
        x = len(column) + 1
        for element in data:
            if element not in column.keys():
                column[element] = x
                x += 1


class CategoricalToNumericalConverter(object):
    """Create a dictionary with labels and its respective number format"""

    def __init__(self):
        self.title = {}
        self.author = {}
        self.publisher = {}
        self.shelf = {}

    def transpose_data(self, dataframe):

        convert_to_numerical(dataframe['TITLE'].tolist(), self.title)
        df = dataframe.replace({'TITLE': self.title})

        convert_to_numerical(df['AUTHOR'].tolist(), self.author)
        df.replace({'AUTHOR': self.author}, inplace=True)

        convert_to_numerical(df['PUBLISHER'].tolist(), self.publisher)
        df.replace({'PUBLISHER': self.publisher}, inplace=True)

        values = []
        col = [i for i in df.columns][INITIAL_POPULAR_SHELF_INDEX:]
        [values.extend(df[i].tolist()) for i in col]

        convert_to_numerical(values, self.shelf)
        [df.replace({i: self.shelf}, inplace=True) for i in col]

        return df


def recommendation_tree():
    """A book recommendation system based in books you read in the past"""

    # Initialize the categorical-numerical library
    converter = CategoricalToNumericalConverter()

    # Books to be parsed
    isbn_list = ['9788532530783', '9788556510785']

    if len(isbn_list) > 0:

        # Fetch all related books ISBN codes information
        information, similarity = set_information(isbn_list)
        # Add all info into a Pandas Dataframe
        books_dataframe = create_dataframe(information)

        # 1° turn of fetching similar books
        information, similarity = set_information(similarity)
        # Add all info into a Pandas Dataframe
        similarity_dataframe = create_dataframe(information)

        # 2° turn of fetching similar books
        information, similarity = set_information(similarity)
        # Add all info into a Pandas Dataframe
        similarity_dataframe = pd.concat([create_dataframe(information), similarity_dataframe], ignore_index=True)

        # 3° turn of fetching similar books
        information, similarity = set_information(similarity)
        # Add all info into a Pandas Dataframe
        similarity_dataframe = pd.concat([create_dataframe(information), similarity_dataframe], ignore_index=True)

        # Remove duplicate values from Pandas Dataframe
        similarity_dataframe.drop_duplicates(subset='ISBN', keep='first', inplace=True)

        numerical_books_dataframe = converter.transpose_data(books_dataframe)
        numerical_similarity_dataframe = converter.transpose_data(similarity_dataframe)

        # Run prediction
        y_pred = run_prediction(numerical_similarity_dataframe, numerical_books_dataframe)

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
    information = []
    similar_books = []
    similar_books_isbn_list = []
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
            information.append(ret)

    # It is needed to avoid breaking in case of missing information
    try:
        for items in information:
            if len(items.similar_books) > 0:
                for similar in items.similar_books:
                    similar_books.append(similar)
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)

    for info in similar_books:
        if (info.isbn13 is not None) and (str(info.isbn13).isnumeric()):
            similar_books_isbn_list.append(int(info.isbn13))
        else:
            int(0)

    similar_books_isbn_list = list(set(similar_books_isbn_list))
    if 0 in similar_books_isbn_list:
        similar_books_isbn_list.remove(0)

    return information, similar_books_isbn_list


def book_information_lookup(isbn, info_queue):
    """Get info about a book"""
    info = False
    try:
        info = good_reads.book(isbn=isbn)
    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
    finally:
        info_queue.put(info)


def create_dataframe(information):
    book_info = []
    df = pd.DataFrame(columns=COLUMNS_NAMES)

    for info in information:

        try:
            temp = []
            _isbn = int(info.isbn13) if info.isbn13 is not None else int(0)
            _gid = int(info.gid) if info.gid is not None else int(0)
            _title = str(info.title) if info.title is not None else str('_')
            _average_rating = float(info.average_rating) if info.average_rating is not None else int(0)
            _ratings_count = int(info.ratings_count) if info.ratings_count is not None else int(0)
            _author = str(info.authors[0]) if info.authors[0] is not None else str('_')
            _publisher = str(info.publisher) if info.publisher is not None else str('_')

            temp.extend([_gid, _isbn, _title, _average_rating, _ratings_count, _author, _publisher])

            for j in info.popular_shelves:
                if len(temp) == QTY_OF_ROWS:
                    break
                temp.append(str(j))

            if len(temp) < QTY_OF_ROWS:
                while len(temp) < QTY_OF_ROWS:
                    temp.append(None)

            if len(temp) > 0:
                book_info.append(temp)

        except Exception as e:
            logger.exception('{}'.format(e), exc_info=False)

    # Add all info into a Pandas Dataframe
    for data in book_info:
        df.loc[len(df)] = data

    return df


def run_prediction(train_database, predict_database):
    """ """

    # Adjust the dataframe information
    col_names = COLUMNS_NAMES[INITIAL_POPULAR_SHELF_INDEX:]
    columns = ['AVERAGE_RATING', 'RATINGS_COUNT', 'AUTHOR', 'PUBLISHER']
    columns.extend(col_names)

    x_train = train_database[columns]
    y_train = train_database['ISBN']
    y_train = y_train.astype('int')

    # Create a Random Forest Classifier
    # logger.info('Running Random Forest Classifier!')
    # clf = RandomForestClassifier(n_estimators=1000)

    logger.info('Running Neural Network!')
    clf = MLPClassifier(hidden_layer_sizes=(100,), random_state=1, max_iter=300, solver='adam', activation='tanh')

    # Create a SVM Classifier
    # logger.info('Running SVM Classifier!')
    # clf = SVC(kernel='rbf')

    # Train the model
    clf.fit(x_train, y_train)

    x_test = predict_database[columns]
    y_pred = clf.predict(x_test)

    return list(set(y_pred))
