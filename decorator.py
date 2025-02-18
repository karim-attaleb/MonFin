from functools import wraps
import time
import helper


# def timer(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         start_time = time.perf_counter()
#         result = func(*args, **kwargs)
#         end_time = time.perf_counter()
#         time_calc = end_time - start_time
#         helper.logger.info(f'Run time was {end_time - start_time} seconds.\n')
#         return result
#
#     return wrapper


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            end_time = time.perf_counter()
            time_calc = end_time - start_time
            helper.logger.info(f'Run time of option was {time_calc} seconds.\n')

    return wrapper



# https://realpython.com/python-sleep/
def sleep(timeout, retry=3):
    def the_real_decorator(function):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < retry:
                try:
                    value = function(*args, **kwargs)
                    if value is None:
                        return
                except:
                    print(f'Sleeping for {timeout} seconds')
                    time.sleep(timeout)
                    retries += 1
        return wrapper
    return the_real_decorator