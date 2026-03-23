class HuaweiApiError(Exception):
    pass


class HuaweiLoginError(HuaweiApiError):
    pass


class HuaweiRateLimitError(HuaweiApiError):
    pass


class HuaweiUnauthorizedError(HuaweiApiError):
    pass