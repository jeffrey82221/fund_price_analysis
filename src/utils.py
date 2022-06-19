from src.exceptions import IterationFailError
import traceback
import gc
def batch_generator(generator, batch_size=10):
    """
    Args:   
        - generator: an generator produce item one by one
        - batch_size: the number of item produced in each iteration of batch_generator
    Yields:
        - a batch of items
    """
    try:
        result = []
        for data in generator:
            result.append(data)
            if len(result) == batch_size:
                yield result
                result = []
                gc.collect()
        if len(result) > 0:
            yield result
    except GeneratorExit as e:
        raise e
    except KeyboardInterrupt as e:
        raise e
    except BaseException:
        print(traceback.format_exc())
        raise IterationFailError()