# Build-in modules
import logging
import os

# Project modules
from Database.file import get_directory_path
from Machine_learning.barcode import barcode_parser
from messages import send_message as send

logger = logging.getLogger(__name__)


def picture_parser(update):
    """
    Download the picture from Telegram server and parser it using cv2.
    :return: (tuple) msg, True/False:   msg - with a message to return.
                                        False - when something went wrong.
                                        True - When the ISBN value is ready to used
    """
    # Get the last picture of the set, highest resolution
    pic_index = -1

    # Get picture information
    filename = update.message.photo[pic_index].file_id + '.jpg'
    user = update.effective_user.full_name
    chat_id = update.message.chat_id

    # Download the picture
    update.message.photo[pic_index].get_file().download(custom_path="./Database/{}".format(filename))
    logger.info("Got photo from %s - %s: %s", chat_id, user, filename)
    send('Parsing your photo. Wait a moment!', update)

    # Process the picture and return a ISBN value or false, otherwise.
    path = get_directory_path()
    if os.path.exists("{}/{}".format(path, filename)):
        # Barcode parser
        data = barcode_parser(filename)
        # Delete the received picture.
        os.remove("{}/{}".format(path, filename))
        # Verify the picture consistence
        if len(data) > 0:
            return data[0].decode("utf-8"), True
        else:
            return 'No barcode was recognized!', False
    else:
        return 'Something went wrong. Please, try again!', False
