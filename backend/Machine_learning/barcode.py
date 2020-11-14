# Build-in modules
import logging

# Added modules
import cv2
import pyzbar.pyzbar as pyzbar
from cv2 import Error

logger = logging.getLogger(__name__)


def decode(im):
    """
    Decode Barcode into a numeric code
    """
    # Find barcode and QR codes
    decoded_objects = pyzbar.decode(im)
    return [obj.data for obj in decoded_objects if len(decoded_objects) > 0]


def barcode_parser(filename):
    """
    Barcode parser
    """
    decoded_objects = []
    try:
        im = cv2.imread('./Database/{}'.format(filename))
        decoded_objects = decode(im)
    except Error as e:
        logger.exception('{}'.format(e))
    finally:
        return decoded_objects
