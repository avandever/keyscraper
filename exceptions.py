class DeckNotFoundError(Exception):
    pass


class UnknownDBDriverException(Exception):
    pass


class RequestThrottled(Exception):
    pass


class InternalServerError(Exception):
    pass
