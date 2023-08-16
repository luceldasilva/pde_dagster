import logging


logging.basicConfig(
    format = '%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s', 
    level=logging.INFO,
    encoding="utf-8"
    )


logger = logging.getLogger(__name__)