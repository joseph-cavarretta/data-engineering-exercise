class ExtractError(Exception):
    """ All custom extract Exceptions """
    pass

class MaxRetriesExceededError(ExtractError):
    """ Raise when API call fails exceed the max_retries count """
    pass