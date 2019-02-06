import logging
import typing


LOG_FORMAT = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'

def get_logger(log_name: str = '', 
               level: typing.Union[str, int] = 'INFO',
               log_format: str = LOG_FORMAT,
               date_format: str = '%Y-%m-%d %H:%M:%S'):
    """Fetches a logger so you don't have to.
    Parameters
    ----------
    log_name : str (optional, defaults: '')
        The name of the logger.
    level : int or str {'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'} (optional)
        The threshold for which the logger will output a message.
        Anything below the threshold gets filtered. Defaults to 'INFO'.
    log_format : str (optional)
        The format of the log messages. Defaults to:
            %(asctime)s %(name)-12s %(levelname)-8s %(message)s
    date_format : str (optional)
        The format of the date component of the message (if any).
        Defaults to: %Y-%m-%d %H:%M:%S
    Returns
    -------
    out : logger
        A logging instance.
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(level)
    logger.propagate = 0

    # If there are no other handlers, create one and set level.
    # Note that logger.handlers is a list of handlers
    if len(logger.handlers) == 0:

        formatter = logging.Formatter(log_format, datefmt=date_format)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    for sh in logger.handlers:
        if isinstance(sh, logging.StreamHandler):
            sh.setLevel(level)

    return logger