from tenacity import retry, stop_after_attempt, wait_exponential, wait_random, retry_if_exception_type
from functools import wraps
import logging

def create_retry_decorator(logger: logging.Logger):
    """Create a retry decorator with logging for transient errors."""
    
    def before_sleep(retry_state):
        logger.warning(
            f"Retrying {retry_state.fn.__name__} after {retry_state.outcome.exception()} "
            f"(attempt {retry_state.attempt_number})"
        )
    
    def retry_decorator(func):
        @wraps(func)
        @retry(
            stop=stop_after_attempt(3),  # Max 3 attempts to retry.
            wait=wait_exponential(multiplier=1, min=4, max=16) + wait_random(0, 2),  # 4s → 16s delays after failures + jitter (random wait).
            retry=retry_if_exception_type((ConnectionError, TimeoutError, RuntimeError)),
            before_sleep=lambda retry_state: logger.warning(
                f"Retrying after {retry_state.outcome.exception()} (attempt {retry_state.attempt_number})"
                ),
            reraise=True
            )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    
    return retry_decorator