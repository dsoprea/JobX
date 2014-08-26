import random

count = random.randrange(1, arg1)

while count > 1:
    interval = random.randrange(1, count)
    count -= interval

    yielded_step_name = 'step5'
    yielded_step_args = {'arg1': interval }

    yield (yielded_step_name, yielded_step_args)
