
class BaseResponseException(Exception):
    '''Thrown When An API Returns a Bad Response Code'''
    def __init__(self, url, response_code):
        self.url = url
        self.response_code = response_code


class TooManyRequestsException(BaseResponseException):
    '''Thrown When We've Made Too Many Calls To The API'''
